/*
 * jQuery JSON-RPC Plugin
 *
 * @version: 0.3(2012-05-17)
 * @author hagino3000 <http://twitter.com/hagino3000> (Takashi Nishibayashi)
 * @author alanjds <http://twitter.com/alanjds> (Alan Justino da Silva)
 *
 * A JSON-RPC 2.0 implementation for jQuery.
 * JSON-RPC is a stateless, light-weight remote procedure call (RPC) protocol.
 * Read more in the <http://groups.google.com/group/json-rpc/web/json-rpc-2-0>
 *
 * Requires json2.js<http://www.json.org/json2.js> if browser has not window.JSON.
 *
 * Usage:
 *   $.jsonrpc(data [, callbacks [, debug]]);
 *
 *   where data = {url: '/rpc/', method:'simplefunc', params:['posi', 'tional']}
 *   or data = {url: '/rpc/', method:'complexfunc', params:{nam:'ed', par:'ams'}}
 *   and callbacks = {success: successFunc, error: errorFunc}
 *
 *   Setting no callback produces a JSON-RPC Notification.
 *   'data' accepts 'timeout' keyword too, who sets the $.ajax request timeout.
 *   Setting 'debug' to true prints responses to Firebug's console.info
 *
 * Examples:
 *   // A RPC call with named parameters
 *   $.jsonrpc({
 *     url : '/rpc',
 *     method : 'createUser',
 *     params : {name : 'John Smith', userId : '1000'}
 *   }, {
 *     success : function(result) {
 *       //doSomething
 *     },
 *     error : function(error) {
 *       //doSomething
 *     }
 *   });
 *
 *   // Once set defaultUrl, url option is no need
 *   $.jsonrpc.defaultUrl = '/rpc';
 *
 *   // A Notification
 *   $.jsonrpc({
 *     method : 'notify',
 *     params : {action : 'logout', userId : '1000'}
 *   });
 *
 *   // A Notification using console to debug and with timeout set
 *   $.jsonrpc({
 *     method : 'notify',
 *     params : {action : 'logout', userId : '1000'},
 *     debug : true,
 *     timeout : 500,
 *   });
 *
 *   // Set DataFilter. It is useful for buggy API that returns sometimes not json but html (when 500, 403..).
 *   $.jsonrpc({
 *     method : 'getUser',
 *     dataFilter : function(data, type) {
 *       try {
 *         return JSON.parse(data);
 *       } catch(e) {
 *         return {error : {message : 'Cannot parse response', data : data}};
 *       }
 *     }, function(result){ doSomething... }
 *   }, {
 *     success : handleSuccess
 *     error : handleFailure
 *   });
 *
 * This document is licensed as free software under the terms of the
 * MIT License: http://www.opensource.org/licenses/mit-license.php
 */
(function($) {

  var rpcid = 1,
      emptyFn = function() {};

  $.jsonrpc = $.jsonrpc || function(data, callbacks, debug) {
    debug = debug || false;

    var postdata = {
      jsonrpc: '2.0',
      method: data.method || '',
      params: data.params || {}
    };
    if (callbacks) {
      postdata.id = data.id || rpcid++;
    } else {
      callbacks = emptyFn;
    }

    if (typeof(callbacks) === 'function') {
      callbacks = {
        success: callbacks,
        error: callbacks
      };
    }

    var dataFilter = data.dataFilter;

    var ajaxopts = {
      url: data.url || $.jsonrpc.defaultUrl,
      contentType: 'application/json',
      dataType: 'text',
      dataFilter: function(data, type) {
        if (dataFilter) {
          return dataFilter(data);
        } else {
		  if (data != "") return JSON.parse(data);
        }
      },
      type: 'POST',
      processData: false,
      data: JSON.stringify(postdata),
      success: function(resp) {
        if (resp && !resp.error) {
          return callbacks.success && callbacks.success(resp.result);
        } else if (resp && resp.error) {
          return callbacks.error && callbacks.error(resp.error);
        } else {
          return callbacks.error && callbacks.error(resp);
        }
      },
      error: function(xhr, status, error) {
        if (error === 'timeout') {
          callbacks.error({
            status: status,
            code: 0,
            message: 'Request Timeout'
          });
          return;
        }
        // If response code is 404, 400, 500, server returns error object
        try {
          var res = JSON.parse(xhr.responseText);
          callbacks.error(res.error);
        } catch (e) {
          callbacks.error({
            status: status,
            code: 0,
            message: error
          });
        }
      }
    };
    if (data.timeout) {
      ajaxopts['timeout'] = data.timeout;
    }

    $.ajax(ajaxopts);

    return $;
  }
  $.jsonrpc.defaultUrl = $.jsonrpc.defaultUrl || '/jsonrpc/';

})(jQuery);
