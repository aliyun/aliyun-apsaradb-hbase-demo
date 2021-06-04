package cn.alibaba.mob.filter;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import java.io.IOException;
import java.util.Enumeration;

@Component
@WebFilter(urlPatterns = "/*", filterName = "postDataDumperFilter")
public class PostDataDumperFilter implements Filter {

    Logger logger = Logger.getLogger(PostDataDumperFilter.class);

    private FilterConfig filterConfig = null;

    public void destroy() {
        this.filterConfig = null;
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        if (filterConfig == null)
            return;

        Enumeration<String> names = request.getParameterNames();
        StringBuilder output = new StringBuilder();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            output.append(name).append("=");
            String values[] = request.getParameterValues(name);
            for (int i = 0; i < values.length; i++) {
                if (i > 0) {
                    output.append("' ");
                }

                output.append(values[i]);
            }
            if (names.hasMoreElements())
                output.append("&");
        }
        request.setAttribute("postdata", output);
        logger.debug("postdata: " + output);
        chain.doFilter(request, response);
    }

    public void init(FilterConfig filterConfig) throws ServletException {
        this.filterConfig = filterConfig;
    }
}
