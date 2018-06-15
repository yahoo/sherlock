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
function ajaxMessage(jqXHR, exception) {
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