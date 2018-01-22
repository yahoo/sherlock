/*
MIT License
Copyright (c) 2017 Sphinxxxx
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

function cmResize(cm, config) {
    config = config || {};

    const minW = config.minWidth  || 200,
          minH = config.minHeight || 100,
          resizeW = (config.resizableWidth  !== false),
          resizeH = (config.resizableHeight !== false);

    const cmElement = cm.display.wrapper,
          cmHandle = config.handle || (function() {
              const h = cmElement.appendChild(document.createElement('div'));
              h.className = 'cm-drag-handle';
              h.style = ''
                    + 'position: absolute;'
                    + 'bottom: 0;'
                    + 'left: 0%;'
                    + 'z-index: 999;'
                    + 'border-right: 12px solid transparent;'
                    + 'border-bottom: 12px solid rgba(0, 0, 0, 0.5);'
                    + 'cursor: pointer;'
                    + 'color: gray;'
                    + 'background: repeating-linear-gradient(135deg, transparent, transparent 2px, currentColor 0, currentColor 4px);'
              ;
              return h;
          })();

    let startX, startY,
        startW, startH;

    function isLeftButton(e) {
        //https://developer.mozilla.org/en-US/docs/Web/API/MouseEvent/buttons
        return (e.buttons !== undefined)
                    ? (e.buttons === 1)
                    : (e.which === 1) /* Safari (not tested) */;
    }

    function onDrag(e) {
        if(!isLeftButton(e)) {
            //Mouseup outside of window:
            onRelease(e);
            return;
        }
        e.preventDefault();

        var w = resizeW ? Math.max(minW, (startW + e.clientX - startX)) : null,
            h = resizeH ? Math.max(minH, (startH + e.clientY - startY)) : null;
        w = startW;
        cm.setSize(w, h);

        //Leave room for our default drag handle when only one scrollbar is visible:
        //if(!config.handle) {
          //  cmElement.querySelector('.CodeMirror-vscrollbar').style.bottom = '15px';
          //  cmElement.querySelector('.CodeMirror-hscrollbar').style.right = '15px';
        //}
    }

    function onRelease(e) {
        e.preventDefault();

        window.removeEventListener("mousemove", onDrag);
        window.removeEventListener("mouseup", onRelease);
    }

    cmHandle.addEventListener("mousedown", function (e) {
        if(!isLeftButton(e)) { return; }
        e.preventDefault();

        startX = e.clientX;
        startY = e.clientY;
        startH = cmElement.offsetHeight;
        startW = cmElement.offsetWidth;

        window.addEventListener("mousemove", onDrag);
        window.addEventListener("mouseup", onRelease);
    });
}