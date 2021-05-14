define(["app/plugins/sdk","lodash"], function(__WEBPACK_EXTERNAL_MODULE_grafana_app_plugins_sdk__, __WEBPACK_EXTERNAL_MODULE_lodash__) { return /******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, { enumerable: true, get: getter });
/******/ 		}
/******/ 	};
/******/
/******/ 	// define __esModule on exports
/******/ 	__webpack_require__.r = function(exports) {
/******/ 		if(typeof Symbol !== 'undefined' && Symbol.toStringTag) {
/******/ 			Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });
/******/ 		}
/******/ 		Object.defineProperty(exports, '__esModule', { value: true });
/******/ 	};
/******/
/******/ 	// create a fake namespace object
/******/ 	// mode & 1: value is a module id, require it
/******/ 	// mode & 2: merge all properties of value into the ns
/******/ 	// mode & 4: return value when already ns object
/******/ 	// mode & 8|1: behave like require
/******/ 	__webpack_require__.t = function(value, mode) {
/******/ 		if(mode & 1) value = __webpack_require__(value);
/******/ 		if(mode & 8) return value;
/******/ 		if((mode & 4) && typeof value === 'object' && value && value.__esModule) return value;
/******/ 		var ns = Object.create(null);
/******/ 		__webpack_require__.r(ns);
/******/ 		Object.defineProperty(ns, 'default', { enumerable: true, value: value });
/******/ 		if(mode & 2 && typeof value != 'string') for(var key in value) __webpack_require__.d(ns, key, function(key) { return value[key]; }.bind(null, key));
/******/ 		return ns;
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = "./module.ts");
/******/ })
/************************************************************************/
/******/ ({

/***/ "../node_modules/tslib/tslib.es6.js":
/*!******************************************!*\
  !*** ../node_modules/tslib/tslib.es6.js ***!
  \******************************************/
/*! exports provided: __extends, __assign, __rest, __decorate, __param, __metadata, __awaiter, __generator, __createBinding, __exportStar, __values, __read, __spread, __spreadArrays, __await, __asyncGenerator, __asyncDelegator, __asyncValues, __makeTemplateObject, __importStar, __importDefault, __classPrivateFieldGet, __classPrivateFieldSet */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
eval("__webpack_require__.r(__webpack_exports__);\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__extends\", function() { return __extends; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__assign\", function() { return __assign; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__rest\", function() { return __rest; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__decorate\", function() { return __decorate; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__param\", function() { return __param; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__metadata\", function() { return __metadata; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__awaiter\", function() { return __awaiter; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__generator\", function() { return __generator; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__createBinding\", function() { return __createBinding; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__exportStar\", function() { return __exportStar; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__values\", function() { return __values; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__read\", function() { return __read; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__spread\", function() { return __spread; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__spreadArrays\", function() { return __spreadArrays; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__await\", function() { return __await; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__asyncGenerator\", function() { return __asyncGenerator; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__asyncDelegator\", function() { return __asyncDelegator; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__asyncValues\", function() { return __asyncValues; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__makeTemplateObject\", function() { return __makeTemplateObject; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__importStar\", function() { return __importStar; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__importDefault\", function() { return __importDefault; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__classPrivateFieldGet\", function() { return __classPrivateFieldGet; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"__classPrivateFieldSet\", function() { return __classPrivateFieldSet; });\n/*! *****************************************************************************\r\nCopyright (c) Microsoft Corporation.\r\n\r\nPermission to use, copy, modify, and/or distribute this software for any\r\npurpose with or without fee is hereby granted.\r\n\r\nTHE SOFTWARE IS PROVIDED \"AS IS\" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH\r\nREGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY\r\nAND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,\r\nINDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM\r\nLOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR\r\nOTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR\r\nPERFORMANCE OF THIS SOFTWARE.\r\n***************************************************************************** */\r\n/* global Reflect, Promise */\r\n\r\nvar extendStatics = function(d, b) {\r\n    extendStatics = Object.setPrototypeOf ||\r\n        ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||\r\n        function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };\r\n    return extendStatics(d, b);\r\n};\r\n\r\nfunction __extends(d, b) {\r\n    extendStatics(d, b);\r\n    function __() { this.constructor = d; }\r\n    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());\r\n}\r\n\r\nvar __assign = function() {\r\n    __assign = Object.assign || function __assign(t) {\r\n        for (var s, i = 1, n = arguments.length; i < n; i++) {\r\n            s = arguments[i];\r\n            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p)) t[p] = s[p];\r\n        }\r\n        return t;\r\n    }\r\n    return __assign.apply(this, arguments);\r\n}\r\n\r\nfunction __rest(s, e) {\r\n    var t = {};\r\n    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)\r\n        t[p] = s[p];\r\n    if (s != null && typeof Object.getOwnPropertySymbols === \"function\")\r\n        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {\r\n            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))\r\n                t[p[i]] = s[p[i]];\r\n        }\r\n    return t;\r\n}\r\n\r\nfunction __decorate(decorators, target, key, desc) {\r\n    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;\r\n    if (typeof Reflect === \"object\" && typeof Reflect.decorate === \"function\") r = Reflect.decorate(decorators, target, key, desc);\r\n    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;\r\n    return c > 3 && r && Object.defineProperty(target, key, r), r;\r\n}\r\n\r\nfunction __param(paramIndex, decorator) {\r\n    return function (target, key) { decorator(target, key, paramIndex); }\r\n}\r\n\r\nfunction __metadata(metadataKey, metadataValue) {\r\n    if (typeof Reflect === \"object\" && typeof Reflect.metadata === \"function\") return Reflect.metadata(metadataKey, metadataValue);\r\n}\r\n\r\nfunction __awaiter(thisArg, _arguments, P, generator) {\r\n    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }\r\n    return new (P || (P = Promise))(function (resolve, reject) {\r\n        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }\r\n        function rejected(value) { try { step(generator[\"throw\"](value)); } catch (e) { reject(e); } }\r\n        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }\r\n        step((generator = generator.apply(thisArg, _arguments || [])).next());\r\n    });\r\n}\r\n\r\nfunction __generator(thisArg, body) {\r\n    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;\r\n    return g = { next: verb(0), \"throw\": verb(1), \"return\": verb(2) }, typeof Symbol === \"function\" && (g[Symbol.iterator] = function() { return this; }), g;\r\n    function verb(n) { return function (v) { return step([n, v]); }; }\r\n    function step(op) {\r\n        if (f) throw new TypeError(\"Generator is already executing.\");\r\n        while (_) try {\r\n            if (f = 1, y && (t = op[0] & 2 ? y[\"return\"] : op[0] ? y[\"throw\"] || ((t = y[\"return\"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;\r\n            if (y = 0, t) op = [op[0] & 2, t.value];\r\n            switch (op[0]) {\r\n                case 0: case 1: t = op; break;\r\n                case 4: _.label++; return { value: op[1], done: false };\r\n                case 5: _.label++; y = op[1]; op = [0]; continue;\r\n                case 7: op = _.ops.pop(); _.trys.pop(); continue;\r\n                default:\r\n                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }\r\n                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }\r\n                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }\r\n                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }\r\n                    if (t[2]) _.ops.pop();\r\n                    _.trys.pop(); continue;\r\n            }\r\n            op = body.call(thisArg, _);\r\n        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }\r\n        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };\r\n    }\r\n}\r\n\r\nvar __createBinding = Object.create ? (function(o, m, k, k2) {\r\n    if (k2 === undefined) k2 = k;\r\n    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });\r\n}) : (function(o, m, k, k2) {\r\n    if (k2 === undefined) k2 = k;\r\n    o[k2] = m[k];\r\n});\r\n\r\nfunction __exportStar(m, o) {\r\n    for (var p in m) if (p !== \"default\" && !Object.prototype.hasOwnProperty.call(o, p)) __createBinding(o, m, p);\r\n}\r\n\r\nfunction __values(o) {\r\n    var s = typeof Symbol === \"function\" && Symbol.iterator, m = s && o[s], i = 0;\r\n    if (m) return m.call(o);\r\n    if (o && typeof o.length === \"number\") return {\r\n        next: function () {\r\n            if (o && i >= o.length) o = void 0;\r\n            return { value: o && o[i++], done: !o };\r\n        }\r\n    };\r\n    throw new TypeError(s ? \"Object is not iterable.\" : \"Symbol.iterator is not defined.\");\r\n}\r\n\r\nfunction __read(o, n) {\r\n    var m = typeof Symbol === \"function\" && o[Symbol.iterator];\r\n    if (!m) return o;\r\n    var i = m.call(o), r, ar = [], e;\r\n    try {\r\n        while ((n === void 0 || n-- > 0) && !(r = i.next()).done) ar.push(r.value);\r\n    }\r\n    catch (error) { e = { error: error }; }\r\n    finally {\r\n        try {\r\n            if (r && !r.done && (m = i[\"return\"])) m.call(i);\r\n        }\r\n        finally { if (e) throw e.error; }\r\n    }\r\n    return ar;\r\n}\r\n\r\nfunction __spread() {\r\n    for (var ar = [], i = 0; i < arguments.length; i++)\r\n        ar = ar.concat(__read(arguments[i]));\r\n    return ar;\r\n}\r\n\r\nfunction __spreadArrays() {\r\n    for (var s = 0, i = 0, il = arguments.length; i < il; i++) s += arguments[i].length;\r\n    for (var r = Array(s), k = 0, i = 0; i < il; i++)\r\n        for (var a = arguments[i], j = 0, jl = a.length; j < jl; j++, k++)\r\n            r[k] = a[j];\r\n    return r;\r\n};\r\n\r\nfunction __await(v) {\r\n    return this instanceof __await ? (this.v = v, this) : new __await(v);\r\n}\r\n\r\nfunction __asyncGenerator(thisArg, _arguments, generator) {\r\n    if (!Symbol.asyncIterator) throw new TypeError(\"Symbol.asyncIterator is not defined.\");\r\n    var g = generator.apply(thisArg, _arguments || []), i, q = [];\r\n    return i = {}, verb(\"next\"), verb(\"throw\"), verb(\"return\"), i[Symbol.asyncIterator] = function () { return this; }, i;\r\n    function verb(n) { if (g[n]) i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; }\r\n    function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }\r\n    function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }\r\n    function fulfill(value) { resume(\"next\", value); }\r\n    function reject(value) { resume(\"throw\", value); }\r\n    function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }\r\n}\r\n\r\nfunction __asyncDelegator(o) {\r\n    var i, p;\r\n    return i = {}, verb(\"next\"), verb(\"throw\", function (e) { throw e; }), verb(\"return\"), i[Symbol.iterator] = function () { return this; }, i;\r\n    function verb(n, f) { i[n] = o[n] ? function (v) { return (p = !p) ? { value: __await(o[n](v)), done: n === \"return\" } : f ? f(v) : v; } : f; }\r\n}\r\n\r\nfunction __asyncValues(o) {\r\n    if (!Symbol.asyncIterator) throw new TypeError(\"Symbol.asyncIterator is not defined.\");\r\n    var m = o[Symbol.asyncIterator], i;\r\n    return m ? m.call(o) : (o = typeof __values === \"function\" ? __values(o) : o[Symbol.iterator](), i = {}, verb(\"next\"), verb(\"throw\"), verb(\"return\"), i[Symbol.asyncIterator] = function () { return this; }, i);\r\n    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }\r\n    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }\r\n}\r\n\r\nfunction __makeTemplateObject(cooked, raw) {\r\n    if (Object.defineProperty) { Object.defineProperty(cooked, \"raw\", { value: raw }); } else { cooked.raw = raw; }\r\n    return cooked;\r\n};\r\n\r\nvar __setModuleDefault = Object.create ? (function(o, v) {\r\n    Object.defineProperty(o, \"default\", { enumerable: true, value: v });\r\n}) : function(o, v) {\r\n    o[\"default\"] = v;\r\n};\r\n\r\nfunction __importStar(mod) {\r\n    if (mod && mod.__esModule) return mod;\r\n    var result = {};\r\n    if (mod != null) for (var k in mod) if (k !== \"default\" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);\r\n    __setModuleDefault(result, mod);\r\n    return result;\r\n}\r\n\r\nfunction __importDefault(mod) {\r\n    return (mod && mod.__esModule) ? mod : { default: mod };\r\n}\r\n\r\nfunction __classPrivateFieldGet(receiver, privateMap) {\r\n    if (!privateMap.has(receiver)) {\r\n        throw new TypeError(\"attempted to get private field on non-instance\");\r\n    }\r\n    return privateMap.get(receiver);\r\n}\r\n\r\nfunction __classPrivateFieldSet(receiver, privateMap, value) {\r\n    if (!privateMap.has(receiver)) {\r\n        throw new TypeError(\"attempted to set private field on non-instance\");\r\n    }\r\n    privateMap.set(receiver, value);\r\n    return value;\r\n}\r\n\n\n//# sourceURL=webpack:///../node_modules/tslib/tslib.es6.js?");

/***/ }),

/***/ "./config_ctrl.ts":
/*!************************!*\
  !*** ./config_ctrl.ts ***!
  \************************/
/*! exports provided: CassandraConfigCtrl */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
eval("__webpack_require__.r(__webpack_exports__);\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"CassandraConfigCtrl\", function() { return CassandraConfigCtrl; });\nvar CassandraConfigCtrl = /** @class */ (function () {\n    function CassandraConfigCtrl() {\n    }\n    CassandraConfigCtrl.templateUrl = 'partials/config.html';\n    return CassandraConfigCtrl;\n}());\n\n\n\n//# sourceURL=webpack:///./config_ctrl.ts?");

/***/ }),

/***/ "./datasource.ts":
/*!***********************!*\
  !*** ./datasource.ts ***!
  \***********************/
/*! exports provided: CassandraDatasource, handleTsdbResponse, mapToTextValue */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
eval("__webpack_require__.r(__webpack_exports__);\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"CassandraDatasource\", function() { return CassandraDatasource; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"handleTsdbResponse\", function() { return handleTsdbResponse; });\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"mapToTextValue\", function() { return mapToTextValue; });\n/* harmony import */ var lodash__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! lodash */ \"lodash\");\n/* harmony import */ var lodash__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(lodash__WEBPACK_IMPORTED_MODULE_0__);\n/* harmony import */ var _models__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ./models */ \"./models.ts\");\n\n\nvar CassandraDatasource = /** @class */ (function () {\n    /** @ngInject */\n    function CassandraDatasource(instanceSettings, backendSrv, templateSrv) {\n        this.backendSrv = backendSrv;\n        this.templateSrv = templateSrv;\n        this.type = instanceSettings.type;\n        this.url = instanceSettings.url;\n        this.keyspace = instanceSettings.jsonData.keyspace;\n        this.user = instanceSettings.jsonData.user;\n        this.name = instanceSettings.name;\n        this.id = instanceSettings.id;\n        this.withCredentials = instanceSettings.withCredentials;\n        this.headers = { 'Content-Type': 'application/json' };\n        if (typeof instanceSettings.basicAuth === 'string' && instanceSettings.basicAuth.length > 0) {\n            this.headers['Authorization'] = instanceSettings.basicAuth;\n        }\n    }\n    CassandraDatasource.prototype.query = function (options) {\n        var query = this.buildQueryParameters(options);\n        query.targets = query.targets.filter(function (t) { return !t.hide; });\n        if (query.targets.length <= 0) {\n            return Promise.resolve({ data: [] });\n        }\n        return this.doTsdbRequest(query).then(handleTsdbResponse);\n    };\n    CassandraDatasource.prototype.testDatasource = function () {\n        return this.backendSrv\n            .datasourceRequest({\n            url: '/api/tsdb/query',\n            method: 'POST',\n            data: {\n                from: '5m',\n                to: 'now',\n                queries: [{ datasourceId: this.id, queryType: 'connection', keyspace: this.keyspace }]\n            },\n        })\n            .then(function () {\n            return { status: 'success', message: 'Database Connection OK' };\n        })\n            .catch(function (error) {\n            return { status: 'error', message: error.data.message };\n        });\n    };\n    CassandraDatasource.prototype.metricFindQuery = function (keyspace, table) {\n        var interpolated = {\n            datasourceId: this.id,\n            queryType: \"search\",\n            refId: \"search\",\n            keyspace: keyspace,\n            table: table\n        };\n        return this.doTsdbRequest({\n            targets: [interpolated]\n        }).then(function (response) {\n            var tmd = new _models__WEBPACK_IMPORTED_MODULE_1__[\"TableMetadata\"](response.data.results.search.tables[\"0\"].rows[\"0\"][\"0\"]);\n            // return tmd.toSuggestion();\n            return tmd;\n        }).catch(function (error) {\n            console.log(error);\n            return new _models__WEBPACK_IMPORTED_MODULE_1__[\"TableMetadata\"]();\n        });\n    };\n    CassandraDatasource.prototype.doRequest = function (options) {\n        options.withCredentials = this.withCredentials;\n        options.headers = this.headers;\n        return this.backendSrv.datasourceRequest(options);\n    };\n    CassandraDatasource.prototype.doTsdbRequest = function (options) {\n        var tsdbRequestData = {\n            queries: options.targets,\n        };\n        if (options.range) {\n            tsdbRequestData.from = options.range.from.valueOf().toString();\n            tsdbRequestData.to = options.range.to.valueOf().toString();\n        }\n        return this.backendSrv.datasourceRequest({\n            url: '/api/tsdb/query',\n            method: 'POST',\n            data: tsdbRequestData\n        });\n    };\n    CassandraDatasource.prototype.buildQueryParameters = function (options) {\n        var _this = this;\n        //remove placeholder targets\n        options.targets = lodash__WEBPACK_IMPORTED_MODULE_0___default.a.filter(options.targets, function (target) {\n            return target.target !== 'select metric';\n        });\n        var targets = lodash__WEBPACK_IMPORTED_MODULE_0___default.a.map(options.targets, function (target) {\n            return {\n                queryType: 'query',\n                target: _this.templateSrv.replace(target.target, options.scopedVars, 'regex'),\n                refId: target.refId,\n                hide: target.hide,\n                rawQuery: target.rawQuery,\n                type: target.type || 'timeserie',\n                datasourceId: _this.id,\n                filtering: target.filtering,\n                keyspace: target.keyspace,\n                table: target.table,\n                columnTime: target.columnTime,\n                columnValue: target.columnValue,\n                columnId: target.columnId,\n                valueId: target.valueId\n            };\n        });\n        options.targets = targets;\n        return options;\n    };\n    return CassandraDatasource;\n}());\n\nfunction handleTsdbResponse(response) {\n    var res = [];\n    lodash__WEBPACK_IMPORTED_MODULE_0___default.a.forEach(response.data.results, function (r) {\n        lodash__WEBPACK_IMPORTED_MODULE_0___default.a.forEach(r.series, function (s) {\n            res.push({ target: s.name, datapoints: s.points });\n        });\n        lodash__WEBPACK_IMPORTED_MODULE_0___default.a.forEach(r.tables, function (t) {\n            t.type = 'table';\n            t.refId = r.refId;\n            res.push(t);\n        });\n    });\n    response.data = res;\n    return response;\n}\nfunction mapToTextValue(result) {\n    return lodash__WEBPACK_IMPORTED_MODULE_0___default.a.map(result, function (d, i) {\n        if (d && d.text && d.value) {\n            return { text: d.text, value: d.value };\n        }\n        else if (lodash__WEBPACK_IMPORTED_MODULE_0___default.a.isObject(d)) {\n            return { text: d, value: i };\n        }\n        return { text: d, value: d };\n    });\n}\n\n\n//# sourceURL=webpack:///./datasource.ts?");

/***/ }),

/***/ "./models.ts":
/*!*******************!*\
  !*** ./models.ts ***!
  \*******************/
/*! exports provided: TableMetadata */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
eval("__webpack_require__.r(__webpack_exports__);\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"TableMetadata\", function() { return TableMetadata; });\n/* harmony import */ var tslib__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! tslib */ \"../node_modules/tslib/tslib.es6.js\");\n\nvar TableMetadata = /** @class */ (function () {\n    function TableMetadata(rawJson) {\n        var e_1, _a;\n        this.columns = [];\n        if (rawJson) {\n            try {\n                for (var _b = Object(tslib__WEBPACK_IMPORTED_MODULE_0__[\"__values\"])(JSON.parse(rawJson)), _c = _b.next(); !_c.done; _c = _b.next()) {\n                    var column = _c.value;\n                    this.columns.push(new ColumnMetadata(column['Name'], column['Type']));\n                }\n            }\n            catch (e_1_1) { e_1 = { error: e_1_1 }; }\n            finally {\n                try {\n                    if (_c && !_c.done && (_a = _b.return)) _a.call(_b);\n                }\n                finally { if (e_1) throw e_1.error; }\n            }\n        }\n    }\n    TableMetadata.prototype.toSuggestion = function () {\n        var e_2, _a;\n        var suggestions = [];\n        try {\n            for (var _b = Object(tslib__WEBPACK_IMPORTED_MODULE_0__[\"__values\"])(this.columns), _c = _b.next(); !_c.done; _c = _b.next()) {\n                var column = _c.value;\n                suggestions.push(column.toSuggestion());\n            }\n        }\n        catch (e_2_1) { e_2 = { error: e_2_1 }; }\n        finally {\n            try {\n                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);\n            }\n            finally { if (e_2) throw e_2.error; }\n        }\n        return suggestions;\n    };\n    return TableMetadata;\n}());\n\nvar ColumnMetadata = /** @class */ (function () {\n    function ColumnMetadata(name, type) {\n        this.name = name;\n        this.type = type;\n    }\n    ColumnMetadata.prototype.toSuggestion = function () {\n        return {\n            text: this.name,\n            value: this.name\n        };\n    };\n    return ColumnMetadata;\n}());\n\n\n//# sourceURL=webpack:///./models.ts?");

/***/ }),

/***/ "./module.ts":
/*!*******************!*\
  !*** ./module.ts ***!
  \*******************/
/*! exports provided: Datasource, ConfigCtrl, QueryCtrl, QueryOptionsCtrl */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
eval("__webpack_require__.r(__webpack_exports__);\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"QueryOptionsCtrl\", function() { return CassandraQueryOptionsCtrl; });\n/* harmony import */ var _datasource__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ./datasource */ \"./datasource.ts\");\n/* harmony reexport (safe) */ __webpack_require__.d(__webpack_exports__, \"Datasource\", function() { return _datasource__WEBPACK_IMPORTED_MODULE_0__[\"CassandraDatasource\"]; });\n\n/* harmony import */ var _query_ctrl__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ./query_ctrl */ \"./query_ctrl.ts\");\n/* harmony reexport (safe) */ __webpack_require__.d(__webpack_exports__, \"QueryCtrl\", function() { return _query_ctrl__WEBPACK_IMPORTED_MODULE_1__[\"CassandraQueryCtrl\"]; });\n\n/* harmony import */ var _config_ctrl__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! ./config_ctrl */ \"./config_ctrl.ts\");\n/* harmony reexport (safe) */ __webpack_require__.d(__webpack_exports__, \"ConfigCtrl\", function() { return _config_ctrl__WEBPACK_IMPORTED_MODULE_2__[\"CassandraConfigCtrl\"]; });\n\n\n\n\nvar CassandraQueryOptionsCtrl = /** @class */ (function () {\n    function CassandraQueryOptionsCtrl() {\n    }\n    CassandraQueryOptionsCtrl.templateUrl = 'partials/query.options.html';\n    return CassandraQueryOptionsCtrl;\n}());\n\n\n\n//# sourceURL=webpack:///./module.ts?");

/***/ }),

/***/ "./query_ctrl.ts":
/*!***********************!*\
  !*** ./query_ctrl.ts ***!
  \***********************/
/*! exports provided: CassandraQueryCtrl */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
eval("__webpack_require__.r(__webpack_exports__);\n/* harmony export (binding) */ __webpack_require__.d(__webpack_exports__, \"CassandraQueryCtrl\", function() { return CassandraQueryCtrl; });\n/* harmony import */ var tslib__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! tslib */ \"../node_modules/tslib/tslib.es6.js\");\n/* harmony import */ var grafana_app_plugins_sdk__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! grafana/app/plugins/sdk */ \"grafana/app/plugins/sdk\");\n/* harmony import */ var grafana_app_plugins_sdk__WEBPACK_IMPORTED_MODULE_1___default = /*#__PURE__*/__webpack_require__.n(grafana_app_plugins_sdk__WEBPACK_IMPORTED_MODULE_1__);\n\n\n//import {TableMetadata} from './models';\nvar CassandraQueryCtrl = /** @class */ (function (_super) {\n    Object(tslib__WEBPACK_IMPORTED_MODULE_0__[\"__extends\"])(CassandraQueryCtrl, _super);\n    /** @ngInject */\n    function CassandraQueryCtrl($scope, $injector) {\n        var _this = _super.call(this, $scope, $injector) || this;\n        _this.hasRawMode = false;\n        _this.scope = $scope;\n        _this.target.target = _this.target.target || 'select timestamp, value from keyspace.table where id=123e4567;';\n        _this.target.type = _this.target.type || 'timeserie';\n        _this.target.columnTime = _this.target.columnTime || ' ';\n        _this.target.columnValue = _this.target.columnValue || ' ';\n        _this.target.columnId = _this.target.columnId || ' ';\n        return _this;\n        // TODO if keyspace and table are set load column suggestions\n    }\n    CassandraQueryCtrl.prototype.getOptions = function (keyspace, table) {\n        if (!keyspace || !table) {\n            return Promise.resolve([]);\n        }\n        return this.datasource.metricFindQuery(keyspace, table).then(function (tmd) {\n            return tmd.toSuggestion();\n        });\n    };\n    CassandraQueryCtrl.prototype.toggleEditorMode = function () {\n        this.target.rawQuery = !this.target.rawQuery;\n    };\n    CassandraQueryCtrl.prototype.onChangeInternal = function () {\n        this.panelCtrl.refresh();\n    };\n    CassandraQueryCtrl.templateUrl = 'partials/query.editor.html';\n    return CassandraQueryCtrl;\n}(grafana_app_plugins_sdk__WEBPACK_IMPORTED_MODULE_1__[\"QueryCtrl\"]));\n\n\n\n//# sourceURL=webpack:///./query_ctrl.ts?");

/***/ }),

/***/ "grafana/app/plugins/sdk":
/*!**********************************!*\
  !*** external "app/plugins/sdk" ***!
  \**********************************/
/*! no static exports found */
/***/ (function(module, exports) {

eval("module.exports = __WEBPACK_EXTERNAL_MODULE_grafana_app_plugins_sdk__;\n\n//# sourceURL=webpack:///external_%22app/plugins/sdk%22?");

/***/ }),

/***/ "lodash":
/*!*************************!*\
  !*** external "lodash" ***!
  \*************************/
/*! no static exports found */
/***/ (function(module, exports) {

eval("module.exports = __WEBPACK_EXTERNAL_MODULE_lodash__;\n\n//# sourceURL=webpack:///external_%22lodash%22?");

/***/ })

/******/ })});;