function encodeQueryData(data) {
    var ret = [];
    for (var d in data) {
        if (data.hasOwnProperty(d)) {
            ret.push(encodeURIComponent(d) + '=' + encodeURIComponent(data[d]));
        }
    }
    return ret.join('&');
}

function codeMirror(id) {
    var textArea = CodeMirror.fromTextArea($(id)[0], {
        matchBrackets: true,
        mode: "application/ld+json",
        lineWrapping: true,
        lineNumbers: true,
        scrollbarStyle: "simple",
        highlightSelectionMatches: {showToken: /\w/, annotateScrollbar: true},
        readOnly: false
    });
    cmResize(textArea);
    return textArea;
}

// Shows an error message box with given message
function showErrorMessage(msg) {
    toastr.error(msg);
}
// Shows an info message box with given message
function showInfoMessage(msg) {
    toastr.info(msg);
}
// Shows a warning message box with given message
function showWarningMessage(msg) {
    toastr.warning(msg);
}
// Return ajax message
function ajaxMessage(jqXHR, exception, error) {
    // Show the error message
    if (jqXHR.status === 0) {
        showErrorMessage('Not connected. Verify network connection.');
    } else if (jqXHR.status == 404) {
        showErrorMessage('Requested page not found. [404]');
    } else if (exception === 'parsererror') {
        showErrorMessage('Requested JSON parse failed.');
    } else if (exception === 'timeout') {
        showErrorMessage('Time out error.');
    } else if (exception === 'abort') {
        showErrorMessage('Ajax request aborted.');
    } else {
        showErrorMessage(jqXHR.responseText);
    }
}

function csrfSafeMethod(method) {
    return (/^(GET|HEAD|OPTIONS)$/.test(method));
}

function getTokenValue(token) {
    var tokens = document.cookie.split(';')
    for (var i = 0; i < tokens.length; i++) {
        var t = tokens[i].trim().split('=')
        if (t.length > 1 && t[0] == token) {
            return t[1]
        }
    }
    return ""
}

$.ajaxSetup({
    beforeSend: function(xhr, settings) {
        if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
            xhr.setRequestHeader("pac4jCsrfToken", getTokenValue("pac4jCsrfToken"));
        }
    }
});
