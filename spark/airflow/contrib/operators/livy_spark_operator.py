import six
import time

from airflow.models import BaseOperator

import requests
from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from requests import exceptions as requests_exceptions
from time import sleep

from airflow.utils.log.logging_mixin import LoggingMixin

try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse

XCOM_RUN_ID_KEY = 'id'
SUBMIT_JOB_ENDPOINT = ('POST', 'batches')
GET_JOB_STATE_ENDPOINT = ['GET', 'batches/{batchId}']
LIVY_HEADER = {'Content-Type': 'application/json'}

class LivySubmitRunOperator(BaseOperator):
    # Used in airflow.models.BaseOperator
    template_fields = ('json',)
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    def __init__(
            self,
            json=None,
            livy_conn=None,
	    polling_period_seconds=30,
            **kwargs):
        """
        Creates a new ``LivySubmitRunOperator``.
        """
        super(LivySubmitRunOperator, self).__init__(**kwargs)
        self.json = json or {}
        self.livy_conn = livy_conn
        # This variable will be used in case our task gets killed.
        self.run_id = None
        self.polling_period_seconds = polling_period_seconds

    def get_hook(self):
        return LivyHook(
            self.livy_conn)

    def execute(self, context):
        hook = self.get_hook()
        self.run_id = hook.submit_run(self.json)
        _handle_livy_operator_execution(self, hook, self.log, context)

    def on_kill(self):
        self.log.info(
            'livy operator not support cancell now.'
        )



class LivyHook(BaseHook, LoggingMixin):
    def __init__(
            self,
            livy_conn=None,
            timeout_seconds=180,
            retry_limit=3,
            retry_delay=1.0):
        """
        :param livy_conn: livy connection url
        :param timeout_seconds:
        :param retry_limit:
        :param retry_delay:
        """
        self.livy_conn = livy_conn
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError('Retry limit must be greater than equal to 1')
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay


    def _do_api_call(self, endpoint_info, json=None):
        """
        Utility function to perform an API call with retries
        :param endpoint_info: Tuple of method and endpoint
        :type endpoint_info: (string, string)
        :param json: Parameters for this API call.
        :type json: dict
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise,
            we throw an AirflowException.
        :rtype: dict
        """
        method, endpoint = endpoint_info
        url = '{host}/{endpoint}'.format(
            host=self.livy_conn,
            endpoint=endpoint)
        if method == 'GET':
            request_func = requests.get
        elif method == 'POST':
            request_func = requests.post
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        attempt_num = 1
        while True:
            try:
                if json is not None:
                    response = request_func(
                        url,
                        json=json,
                        headers=LIVY_HEADER,
                        timeout=self.timeout_seconds)
                else:
                    response = request_func(
                        url,
                        headers=LIVY_HEADER,
                        timeout=self.timeout_seconds)
                response.raise_for_status()
                return response.json()
            except requests_exceptions.RequestException as e:
                if not _retryable_error(e):
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    raise AirflowException('Response: {0}, Status Code: {1}'.format(
                        e.response.content, e.response.status_code))

                self._log_request_error(attempt_num, e)

            if attempt_num == self.retry_limit:
                raise AirflowException(('API requests to LivyServer failed {} times. ' +
                                        'Giving up.').format(self.retry_limit))

            attempt_num += 1
            sleep(self.retry_delay)

    def _log_request_error(self, attempt_num, error):
        self.log.error(
            'Attempt %s API Request to Livy failed with reason: %s',
            attempt_num, error
        )

    def submit_run(self, json):
        """
        Post /batches submit job
        """
        response = self._do_api_call(SUBMIT_JOB_ENDPOINT, json)
        self.log.info('livy id  %s run info %s', response['id'], response)
        return response['id']


    def get_run_state(self, run_id):
        GET_JOB_STATE_ENDPOINT[1] = GET_JOB_STATE_ENDPOINT[1].replace("{batchId}", str(run_id))
        response = self._do_api_call(GET_JOB_STATE_ENDPOINT)
        state = response['state']
        self.log.info('runid  %s , appid %s ,app url %s', response['id'], response['appId'], response['appInfo']['sparkUiUrl'])
        return RunState(state, '')

def _retryable_error(exception):
    return isinstance(exception, requests_exceptions.ConnectionError) \
        or isinstance(exception, requests_exceptions.Timeout) \
        or exception.response is not None and exception.response.status_code >= 500


RUN_LIFE_CYCLE_STATES = [
    'starting',
    'running',
    'success',
    'killed',
    'dead'
]


class RunState:
    """
    Utility class for the run state concept of LivyServer runs.
    """
    def __init__(self, result_state, state_message):
        self.result_state = result_state
        self.state_message = state_message

    # starting、running、success、killed、dead

    @property
    def is_terminal(self):
        if self.result_state not in RUN_LIFE_CYCLE_STATES:
            raise AirflowException(
                ('Unexpected life cycle state: {}: If the state has '
                 'been introduced recently, please check the LivyServer user '
                 'guide for troubleshooting information').format(
                    self.result_state))
        return self.result_state in ('success', 'killed', 'dead')

    @property
    def is_successful(self):
        return self.result_state == 'success'

    def __eq__(self, other):
        return self.result_state == other.result_state and \
            self.state_message == other.state_message

    def __repr__(self):
        return str(self.__dict__)



def _handle_livy_operator_execution(operator, hook, log, context):
    """
    Handles the Airflow + Livy lifecycle logic for a Livy operator
    :param operator: Livy operator being handled
    :param context: Airflow context
    """
    while True:
        run_state = hook.get_run_state(operator.run_id)
        if run_state.is_terminal:
            if run_state.is_successful:
                log.info('%s completed successfully.', operator.task_id)
                return
            else:
                error_message = '{t} failed with terminal state: {s}'.format(
                    t=operator.task_id,
                    s=run_state)
                raise AirflowException(error_message)
        else:
            log.info('%s in run state: %s', operator.task_id, run_state)
            log.info('Sleeping for %s seconds.', operator.polling_period_seconds)
            time.sleep(operator.polling_period_seconds)




