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