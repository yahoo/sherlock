/*
The MIT License (MIT)
Copyright (c) 2016 Darragh Kirwan
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

function calendarHeatmap() {
    // defaults
    var LAG_IN_HOUR = 0;
    var frequency = 'day';
    var width = 1150;
    var height = 140;
    var selector = 'body';
    var SQUARE_LENGTH = 16;
    var SQUARE_PADDING = 3;
    var legendSpacing = 7;
    var MONTH_LABEL_PADDING = 6;
    var DAY_LABEL_PADDING = 6;
    var HEATMAP_PADDING = 45
    var data = [];
    var max = null;
    var colorMap = new Map();
    colorMap.set("", '#e1e1e1');
    colorMap.set("success", '#21E3A3');
    colorMap.set("warning", '#ffcc00');
    colorMap.set("error", '#ff0000');
    colorMap.set("nodata", '#f39b9b');
    var tooltipEnabled = true;
    var legendEnabled = true;
    var onClick = null;
    var weekStart = 0; //0 for Sunday, 1 for Monday
    var locale = {
        months: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        days: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
        Anomaly: 'Anomaly',
        defaultMsg: 'No Job Scheduled',
        Error: 'Error',
        NoAnomaly: 'No Anomaly',
        NoData: 'No Data for Timeseries',
        for_period: 'for period'
    };
    var locale_hour = {
        days: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
        hours: ["12-4am", "4-8am", "8-12pm", "12-4pm", "4-8pm", "8-12am"],
        Anomaly: 'Anomaly',
        defaultMsg: 'No Job Scheduled',
        Error: 'Error',
        NoAnoamly: 'No Anomaly',
        NoData: 'No Data for Timeseries',
        for_period: 'for period'
    };

    // setters and getters
    chart.LAG_IN_HOUR = function (value) {
            if (!arguments.length) { return LAG_IN_HOUR; }
            LAG_IN_HOUR = value;
            return chart;
    };

    chart.frequency = function (value) {
        if (!arguments.length) { return frequency; }
        frequency = value;
        return chart;
    };

    chart.data = function (value) {
        if (!arguments.length) { return data; }
        data = value;
        return chart;
    };

    chart.max = function (value) {
        if (!arguments.length) { return max; }
        max = value;
        return chart;
    };

    chart.selector = function (value) {
        if (!arguments.length) { return selector; }
        selector = value;
        return chart;
    };

    chart.colorRange = function (value) {
        if (!arguments.length) { return colorRange; }
        colorRange = value;
        return chart;
    };

    chart.tooltipEnabled = function (value) {
        if (!arguments.length) { return tooltipEnabled; }
        tooltipEnabled = value;
        return chart;
    };

    chart.tooltipUnit = function (value) {
        if (!arguments.length) { return tooltipUnit; }
        tooltipUnit = value;
        return chart;
    };

    chart.legendEnabled = function (value) {
        if (!arguments.length) { return legendEnabled; }
        legendEnabled = value;
        return chart;
    };

    chart.onClick = function (value) {
        if (!arguments.length) { return onClick(); }
        onClick = value;
        return chart;
    };

    chart.locale = function (value) {
        if (!arguments.length) { return locale; }
        locale = value;
        return chart;
    };

    chart.locale_hour = function (value) {
        if (!arguments.length) { return locale_hour; }
        locale_hour = value;
        return chart;
    };


    function chart() {

        d3.select(chart.selector()).selectAll('svg.calendar-heatmap').remove(); // remove the existing chart, if it exists

        var now = moment.utc(moment()).subtract(LAG_IN_HOUR, 'hour').toDate();
        var yearAgo = moment.utc(moment()).startOf('month').subtract(1, 'year').toDate();
        var twoWeeksAgo = moment.utc(moment()).startOf('day').subtract(2, 'week').toDate();

        var yearRange = d3.utcDays(yearAgo, now); // generates an array of daily date objects within the specified range for daily heatmap
        var firstDate = moment.utc(yearRange[0]);

        var twoWeeksRange = d3.utcHours(twoWeeksAgo, now); // generates an array of hourly date objects within the specified range for hourly heatmap
        var firstDateForHour = moment.utc(twoWeeksRange[0]);

        var weekIntervalRange = d3.utcMondays(yearAgo, moment.utc(now).subtract(1, 'week')); // generates an array of weekly date objects within the specified range for weekly heatmap
        var firstDateForWeek = moment.utc(weekIntervalRange[0]);

        var monthRange = d3.utcMonths(yearAgo, now); // generates an array of monthly date objects within the specified range for month labels
        var dayRange = d3.utcDays(twoWeeksAgo, now); // generates an array of daily date objects within the specified range for day labels

        var tooltip;
        var dayRects;
        var hourRects;
        var weekRects;
        var monthRects;

        if (frequency === "hour") {
            drawHourlyChart();
        } else if (frequency === "day") {
            drawDailyChart();
        } else if (frequency === "week") {
            weekStart = 1;
            drawWeeklyChart();
        } else if (frequency === "month") {
            drawMonthyChart();
        }

        // Draw the hourly chart
        function drawHourlyChart() {

            height = 6 * (SQUARE_LENGTH + SQUARE_PADDING) + 10;

            var svg = d3.select(chart.selector())
                .style('position', 'relative')
                .append('svg')
                .attr('transform', 'translate(0,0)')
                .attr('width', width)
                .attr('class', 'calendar-heatmap')
                .attr('height', height)
                .style('padding', '50px');

            hourRects = svg.selectAll('.cell')
                .data(twoWeeksRange);

            hourRects.enter().append('rect')
                .attr('class', 'cell')
                .attr('width', SQUARE_LENGTH)
                .attr('height', SQUARE_LENGTH)
                .attr('fill', function(d) { return colorMap.get(typeForDate(d)); })
                .attr('x', function (d, i) {
                    var cellDate = moment.utc(d);
                    var result = (cellDate.diff(firstDateForHour, 'day') * 4 + (cellDate.hour() % 4)) * (SQUARE_LENGTH + SQUARE_PADDING);
                    return result;
                })
                .attr('y', function (d, i) {
                    var cellDate = moment.utc(d);
                    return DAY_LABEL_PADDING + Math.floor(cellDate.hour() / 4) * (SQUARE_LENGTH + SQUARE_PADDING);
                });

            if (typeof onClick === 'function') {
                hourRects.on('click', function (d) {
                    svg.selectAll('.cell').attr('stroke', null).attr('stroke-width', null);
                    var selection = d3.select(this);
                    selection.attr('stroke', '#08c');
                    selection.attr('stroke-width', '2px');
                    onClick({timestamp: getDateTimestamp(d)});
                });
            }

            if (chart.tooltipEnabled()) {
                hourRects.on('mouseover', function (d, i) {
                    var cellDate = moment.utc(d);
                    tooltip = d3.select(chart.selector())
                        .append('div')
                        .attr('class', 'cell-tooltip')
                        .html(tooltipHTMLForDate(d))
                        .style('left', function () { return Math.floor(i / 6) * SQUARE_LENGTH + 'px'; })
                        .style('top', function () {
	                        const height = d3.select('.cell-tooltip').node().getBoundingClientRect().height;
                            return Math.floor(cellDate.hour() / 4) * (SQUARE_LENGTH + SQUARE_PADDING) + DAY_LABEL_PADDING + HEATMAP_PADDING - height + 'px';
                        });
                })
                .on('mouseout', function (d, i) {
                    tooltip.remove();
                });
            }

            if (chart.legendEnabled()) {
                var colorRange = [];
                colorMap.forEach(function(value, key, map) {
                    colorRange.push({key: key, value: value})
                });

                var legendGroup = svg.selectAll('.calendar-heatmap-legend').data(colorRange);

                legendGroup.enter()
                    .append('g')
                    .attr("class", "legend");

                legendGroup.append('rect')
                    .attr('class', 'calendar-heatmap-legend')
                    .attr('width', SQUARE_LENGTH)
                    .attr('height', SQUARE_LENGTH)
                    .attr('x', function (d, i) { return i * ((SQUARE_LENGTH + SQUARE_PADDING) * legendSpacing); })
                    .attr('y', height + SQUARE_PADDING + SQUARE_LENGTH)
                    .attr('fill', function (d) { return d.value; });

                legendGroup.append('text')
                    .attr('class', 'calendar-heatmap-legend-text calendar-heatmap-legend-text-less')
                    .text(function(d) { return (d.key === 'warning' ? locale.Anomaly : (d.key === 'success' ? locale.NoAnomaly : (d.key === 'error' ? locale.Error : (d.key === 'nodata' ? locale.NoData : locale.defaultMsg)))); })
                    .attr('x', function (d, i) { return (SQUARE_LENGTH + SQUARE_PADDING) + i * ((SQUARE_LENGTH + SQUARE_PADDING) * legendSpacing); })
                    .attr('y', height + 2 * SQUARE_LENGTH)
            }

            hourRects.exit().remove();
            var dayLabels = svg.selectAll('.day')
                .data(dayRange)
                .enter().append('text')
                .attr('class', 'month-name')
                .style()
                .text(function (d) {
                    return locale_hour.days[formatWeekday(d)];
                })
                .attr('x', function (d, i) {
                    var cellDate = moment.utc(d);
                    var matchIndex = 0;
                    twoWeeksRange.find(function (element, index) {
                        matchIndex = index;
                        return cellDate.isSame(element, 'day');
                    });
                    return Math.floor(matchIndex / 6) * (SQUARE_LENGTH + SQUARE_PADDING);
                })
                .attr('y', 0);

            var bars = svg.selectAll(".bars")
                .data(twoWeeksRange)
                .enter().append("path")
                .attr("class", "bars")
                .attr("d", dayPath);

            locale_hour.hours.forEach(function (hour, index) {
                svg.append('text')
                    .attr('class', 'day-initial')
                    .attr('transform', 'translate(-46,' + (SQUARE_LENGTH + SQUARE_PADDING) * (index + 1) + ')')
                    .style('text-anchor', 'left')
                    .attr('dy', '2')
                    .text(hour);
            });

            function dayPath(d) {
                var index = moment.utc(d).diff(firstDateForHour, 'day'),
                    size = SQUARE_LENGTH + SQUARE_PADDING;
                return "M" + (index * 4 * size - 2) + "," + (MONTH_LABEL_PADDING - 2) + "H" + ((index + 1) * 4 * size - 1) + "V" + (MONTH_LABEL_PADDING - 1 + (6 * size)) + "H" + (index * 4 * size - 2) + "Z";
            }
        }

        // Draw the daily chart
        function drawDailyChart() {

            height = 7 * (SQUARE_LENGTH + SQUARE_PADDING) + 10;

            var svg = d3.select(chart.selector())
                .style('position', 'relative')
                .append('svg')
                .attr('transform', 'translate(0,0)')
                .attr('width', width)
                .attr('class', 'calendar-heatmap')
                .attr('height', height)
                .style('padding', '50px');

            dayRects = svg.selectAll('.cell')
                .data(yearRange);

            dayRects.enter().append('rect')
                .attr('class', 'cell')
                .attr('width', SQUARE_LENGTH)
                .attr('height', SQUARE_LENGTH)
                .attr('fill', function(d) { return colorMap.get(typeForDate(d)); })
                .attr('x', function (d, i) {
                    var cellDate = moment.utc(d);
                    var result = cellDate.week() - firstDate.week() + (firstDate.weeksInYear() * (cellDate.weekYear() - firstDate.weekYear()));
                    return result * (SQUARE_LENGTH + SQUARE_PADDING);
                })
                .attr('y', function (d, i) {
                    return MONTH_LABEL_PADDING + formatWeekday(d) * (SQUARE_LENGTH + SQUARE_PADDING);
                });

            if (typeof onClick === 'function') {
                dayRects.on('click', function (d) {
                    svg.selectAll('.cell').attr('stroke', null).attr('stroke-width', null);
                    var selection = d3.select(this);
                    selection.attr('stroke', '#08c');
                    selection.attr('stroke-width', '2px');
	                onClick({timestamp: getDateTimestamp(d)});
                });
            }

            if (chart.tooltipEnabled()) {
                dayRects.on('mouseover', function (d, i) {
                    tooltip = d3.select(chart.selector())
                    .append('div')
                    .attr('class', 'cell-tooltip')
                    .html(tooltipHTMLForDate(d))
                    .style('left', function () { return Math.floor(i / 7) * SQUARE_LENGTH + 'px'; })
                    .style('top', function () {
	                    const height = d3.select('.cell-tooltip').node().getBoundingClientRect().height;
                        return formatWeekday(d) * (SQUARE_LENGTH + SQUARE_PADDING) + MONTH_LABEL_PADDING + HEATMAP_PADDING - height + 'px';
                    });
                })
                .on('mouseout', function (d, i) {
                    tooltip.remove();
                });
            }

            if (chart.legendEnabled()) {
                var colorRange = [];
                colorMap.forEach(function(value, key, map) {
                    colorRange.push({key: key, value: value})
                });

                var legendGroup = svg.selectAll('.calendar-heatmap-legend').data(colorRange);

                legendGroup.enter()
                    .append('g')
                    .attr("class", "legend");

                legendGroup.append('rect')
                    .attr('class', 'calendar-heatmap-legend')
                    .attr('width', SQUARE_LENGTH)
                    .attr('height', SQUARE_LENGTH)
                    .attr('x', function (d, i) { return i * ((SQUARE_LENGTH + SQUARE_PADDING) * legendSpacing); })
                    .attr('y', height + SQUARE_PADDING + SQUARE_LENGTH)
                    .attr('fill', function (d) { return d.value; });

                legendGroup.append('text')
                    .attr('class', 'calendar-heatmap-legend-text calendar-heatmap-legend-text-less')
                    .text(function(d) { return (d.key === 'warning' ? locale.Anomaly : (d.key === 'success' ? locale.NoAnomaly : (d.key === 'error' ? locale.Error : (d.key === 'nodata' ? locale.NoData : locale.defaultMsg)))); })
                    .attr('x', function (d, i) { return (SQUARE_LENGTH + SQUARE_PADDING) + i * ((SQUARE_LENGTH + SQUARE_PADDING) * legendSpacing); })
                    .attr('y', height + 2 * SQUARE_LENGTH)
            }

            dayRects.exit().remove();
            var monthLabels = svg.selectAll('.month')
                .data(monthRange)
                .enter().append('text')
                .attr('class', 'month-name')
                .style()
                .text(function (d) {
                    return locale.months[moment.utc(d).month()];
                })
                .attr('x', function (d, i) {
                    var scale = 0;
                    var diff = moment.utc(d).diff(firstDate, 'day');
                    if (diff > 0) {
                        scale = Math.floor((diff - (7 - (firstDate.isoWeekday() % 7))) / 7);
                        if (formatWeekday(d) == 0) {
                            scale += 1;
                        } else {
                            scale += 2;
                        }
                    }
                    return scale * (SQUARE_LENGTH + SQUARE_PADDING);
                })
                .attr('y', 0);

            var bars = svg.selectAll(".bars")
                  .data(monthRange)
                  .enter().append("path")
                  .attr("class", "bars")
                  .attr("d", monthPath);

            locale.days.forEach(function (day, index) {
                svg.append('text')
                    .attr('class', 'day-initial')
                    .attr('transform', 'translate(-30,' + (SQUARE_LENGTH + SQUARE_PADDING) * (index + 1) + ')')
                    .style('text-anchor', 'left')
                    .attr('dy', '2')
                    .text(day);
            });


            function monthPath(d) {
                var start = moment.utc(d),
                    end = moment.utc(d).endOf('month'),
                    startOffsetX = start.week() - firstDate.week() + (firstDate.weeksInYear() * (start.weekYear() - firstDate.weekYear())),
                    endOffsetX = end.week() - firstDate.week() + (firstDate.weeksInYear() * (end.weekYear() - firstDate.weekYear())),
                    startX = startOffsetX * (SQUARE_LENGTH + SQUARE_PADDING) - 2,
                    startY = MONTH_LABEL_PADDING + (start.isoWeekday() % 7) * (SQUARE_LENGTH + SQUARE_PADDING) - 2,
                    endX = (endOffsetX + 1) * (SQUARE_LENGTH + SQUARE_PADDING) - 1,
                    endY = MONTH_LABEL_PADDING + ((end.isoWeekday() % 7) + 1) * (SQUARE_LENGTH + SQUARE_PADDING) - 1;
                    size = SQUARE_LENGTH + SQUARE_PADDING;
                return "M" + startX + "," + startY + "H" + (startX + size) + "V" + (MONTH_LABEL_PADDING - 2) + "H" + endX + "V" + endY + "H" + (endX - size)
                    + "V" + (MONTH_LABEL_PADDING - 1 + (7 * size)) + "H" + startX + "Z";
            }
        }

        // Draw the weekly chart
        function drawWeeklyChart() {

            SQUARE_PADDING = 4;
            height = 7 * (SQUARE_LENGTH + SQUARE_PADDING) + 10;

            var svg = d3.select(chart.selector())
                .style('position', 'relative')
                .append('svg')
                .attr('transform', 'translate(0,0)')
                .attr('width', width)
                .attr('class', 'calendar-heatmap')
                .attr('height', height)
                .style('padding', '50px');

            weekRects = svg.selectAll('.cell')
                .data(weekIntervalRange);

            weekRects.enter().append('rect')
                .attr('class', 'cell')
                .attr('width', SQUARE_LENGTH)
                .attr('height', 7 * (SQUARE_LENGTH + SQUARE_PADDING))
                .attr('fill', function(d) { return colorMap.get(typeForDate(d)); })
                .attr('x', function (d, i) {
                    var cellDate = moment.utc(d);
                    var result = cellDate.diff(firstDateForWeek , 'week');
                    return result * (SQUARE_LENGTH + SQUARE_PADDING);
                })
                .attr('y', function (d, i) {
                    return MONTH_LABEL_PADDING;
                });

            if (typeof onClick === 'function') {
                weekRects.on('click', function (d) {
                    svg.selectAll('.cell').attr('stroke', null).attr('stroke-width', null);
                    var selection = d3.select(this);
                    selection.attr('stroke', '#08c');
                    selection.attr('stroke-width', '2px');
	                onClick({timestamp: getDateTimestamp(d)});
                });
            }

            if (chart.tooltipEnabled()) {
                weekRects.on('mouseover', function (d, i) {
                    tooltip = d3.select(chart.selector())
                        .append('div')
                        .attr('class', 'cell-tooltip')
                        .html(tooltipHTMLForDate(d))
                        .style('left', function () { return i * (SQUARE_LENGTH + SQUARE_PADDING) + 'px'; })
                        .style('top', function () {
	                        const height = d3.select('.cell-tooltip').node().getBoundingClientRect().height;
	                        return MONTH_LABEL_PADDING + HEATMAP_PADDING - height + 'px';
                        });
                })
                .on('mouseout', function (d, i) {
                    tooltip.remove();
                });
            }

            if (chart.legendEnabled()) {
                var colorRange = [];
                colorMap.forEach(function(value, key, map) {
                    colorRange.push({key: key, value: value})
                });

                var legendGroup = svg.selectAll('.calendar-heatmap-legend').data(colorRange);

                legendGroup.enter()
                    .append('g')
                    .attr("class", "legend");

                legendGroup.append('rect')
                    .attr('class', 'calendar-heatmap-legend')
                    .attr('width', SQUARE_LENGTH)
                    .attr('height', SQUARE_LENGTH)
                    .attr('x', function (d, i) { return i * ((SQUARE_LENGTH + SQUARE_PADDING) * legendSpacing); })
                    .attr('y', height + SQUARE_PADDING + SQUARE_LENGTH)
                    .attr('fill', function (d) { return d.value; });

                legendGroup.append('text')
                    .attr('class', 'calendar-heatmap-legend-text calendar-heatmap-legend-text-less')
                    .text(function(d) { return (d.key === 'warning' ? locale.Anomaly : (d.key === 'success' ? locale.NoAnomaly : (d.key === 'error' ? locale.Error : (d.key === 'nodata' ? locale.NoData : locale.defaultMsg)))); })
                    .attr('x', function (d, i) { return (SQUARE_LENGTH + SQUARE_PADDING) + i * ((SQUARE_LENGTH + SQUARE_PADDING) * legendSpacing); })
                    .attr('y', height + 2 * SQUARE_LENGTH)
            }

            weekRects.exit().remove();
            var monthLabels = svg.selectAll('.month')
                .data(monthRange)
                .enter().append('text')
                .attr('class', 'month-name')
                .style()
                .text(function (d) {
                    return locale.months[moment.utc(d).month()];
                })
                .attr('x', function (d, i) {
                    var scale = 0;
                    var diff = moment.utc(d).diff(firstDateForWeek, 'day');
                    if (diff > 0) {
                        scale = Math.floor(diff / 7);
                    }
                    return scale * (SQUARE_LENGTH + SQUARE_PADDING);
                })
                .attr('y', 0);

            var bars = svg.selectAll(".bars")
                .data(monthRange)
                .enter().append("path")
                .attr("class", "bars")
                .attr("d", weekPath);

            locale.days.forEach(function (day, index) {
                if (weekStart === 1) { index = (index === 0) ? 6 : index - 1;}
                svg.append('text')
                    .attr('class', 'day-initial')
                    .attr('transform', 'translate(-30,' + (SQUARE_LENGTH + SQUARE_PADDING) * (index + 1) + ')')
                    .style('text-anchor', 'left')
                    .attr('dy', '2')
                    .text(day);
            });

            function weekPath(d) {
                var lastIndex = moment.utc(weekIntervalRange[weekIntervalRange.length - 1]).diff(moment.utc(monthRange[0]), 'week');
                    scale = 0,
                    diff = moment.utc(d).diff(firstDateForWeek, 'day'),
                    size = SQUARE_LENGTH + SQUARE_PADDING;
                if (diff > 0) {
                    scale = Math.floor(diff / 7);
                }
                return "M" + (scale * size - 2) + "," + (MONTH_LABEL_PADDING - 2) + "H" + ((lastIndex + 1) * size - 2) + "V" + (MONTH_LABEL_PADDING + 2 + (7 * size)) + "H" + (scale * size - 2) + "Z";
            }
        }

        // Draw the monthly chart
        function drawMonthyChart() {

            monthRange = d3.utcMonths(moment.utc(yearAgo).subtract(1, 'month') , moment.utc(now).subtract(1, 'month'));
            height = 4 * (SQUARE_LENGTH + SQUARE_PADDING) + 10;

            var svg = d3.select(chart.selector())
                .style('position', 'relative')
                .append('svg')
                .attr('transform', 'translate(0,0)')
                .attr('width', width)
                .attr('class', 'calendar-heatmap')
                .attr('height', height)
                .style('padding', '50px');

            monthRects = svg.selectAll('.cell')
                .data(monthRange);

            monthRects.enter().append('rect')
                .attr('class', 'cell')
                .attr('width', 4 * (SQUARE_LENGTH + SQUARE_PADDING) - SQUARE_PADDING)
                .attr('height', 4 * (SQUARE_LENGTH + SQUARE_PADDING))
                .attr('fill', function(d) { return colorMap.get(typeForDate(d)); })
                .attr('x', function (d, i) {
                    return i * 4 * (SQUARE_LENGTH + SQUARE_PADDING) + i * 3;
                })
                .attr('y', function (d, i) {
                    return MONTH_LABEL_PADDING;
                });

            if (typeof onClick === 'function') {
                monthRects.on('click', function (d) {
                    svg.selectAll('.cell').attr('stroke', null).attr('stroke-width', null);
                    var selection = d3.select(this);
                    selection.attr('stroke', '#08c');
                    selection.attr('stroke-width', '2px');
	                onClick({timestamp: getDateTimestamp(d)});
                });
            }

            if (chart.tooltipEnabled()) {
                monthRects.on('mouseover', function (d, i) {
                tooltip = d3.select(chart.selector())
                    .append('div')
                    .attr('class', 'cell-tooltip')
                    .html(tooltipHTMLForDate(d))
                    .style('left', function () { return (i * 4 * (SQUARE_LENGTH + SQUARE_PADDING) + i * 3) + 'px'; })
                    .style('top', function () {
	                    const height = d3.select('.cell-tooltip').node().getBoundingClientRect().height;
                        return MONTH_LABEL_PADDING + HEATMAP_PADDING - height + 'px';
                    });
                })
                .on('mouseout', function (d, i) {
                    tooltip.remove();
                });
            }

            if (chart.legendEnabled()) {
                var colorRange = [];
                colorMap.forEach(function(value, key, map) {
                    colorRange.push({key: key, value: value})
                });

                var legendGroup = svg.selectAll('.calendar-heatmap-legend').data(colorRange);

                legendGroup.enter()
                    .append('g')
                    .attr("class", "legend");

                legendGroup.append('rect')
                    .attr('class', 'calendar-heatmap-legend')
                    .attr('width', SQUARE_LENGTH)
                    .attr('height', SQUARE_LENGTH)
                    .attr('x', function (d, i) { return i * ((SQUARE_LENGTH + SQUARE_PADDING) * legendSpacing); })
                    .attr('y', height + SQUARE_PADDING + SQUARE_LENGTH)
                    .attr('fill', function (d) { return d.value; });

                legendGroup.append('text')
                  .attr('class', 'calendar-heatmap-legend-text calendar-heatmap-legend-text-less')
                  .text(function(d) { return (d.key === 'warning' ? locale.Anomaly : (d.key === 'success' ? locale.NoAnomaly : (d.key === 'error' ? locale.Error : (d.key === 'nodata' ? locale.NoData : locale.defaultMsg)))); })
                  .attr('x', function (d, i) { return (SQUARE_LENGTH + SQUARE_PADDING) + i * ((SQUARE_LENGTH + SQUARE_PADDING) * legendSpacing); })
                  .attr('y', height + 2 * SQUARE_LENGTH)
            }

            var bars = svg.selectAll(".bars")
                .data(monthRange)
                .enter().append("path")
                .attr("class", "bars")
                .attr("d", monthPath);

            monthRects.exit().remove();
            var monthLabels = svg.selectAll('.month')
                .data(monthRange)
                .enter().append('text')
                .attr('class', 'month-name')
                .style()
                .text(function (d) {
                    var utcDate = moment.utc(d);
                    return locale.months[utcDate.month()] + "'" + String(utcDate.format('YYYY')).substring(2,4);
                })
                .attr('x', function (d, i) {
                    return i * 4 * (SQUARE_LENGTH + SQUARE_PADDING) + i * 3;
                })
                .attr('y', 0);

            function monthPath(d) {
                var index = moment.utc(d).diff(monthRange[0], 'month'),
                    size = 4 * (SQUARE_LENGTH + SQUARE_PADDING);
                return "M" + (index * (size + 3) - 3) + "," + (MONTH_LABEL_PADDING - 2) + "H" + ((index + 1) * (size + 3) - 3) + "V" + (MONTH_LABEL_PADDING + 2 + size) + "H" + (index * (size + 3) - 3) + "Z";
            }
        }
    }

    // Function to show tooltip on mouseover
    function tooltipHTMLForDate(d) {
        var dateformat = 'DD MMM HH:00';
        var end = moment.utc(d).add(1, frequency).format(dateformat);
        var start = moment.utc(d).format(dateformat);
        var type = typeForDate(d);
        return '<span><span style="color:' + colorMap.get(type) + '"><strong>'
               + (type === 'warning' ? locale.Anomaly :
                 (type === 'success' ? locale.NoAnomaly :
                 (type === 'error' ? locale.Error :
                 (type === 'nodata' ? locale.NoData : locale.defaultMsg))))
               + ' ' + '</strong></span>' + locale.for_period + ' ' + start + ' to ' + end + '</span>';
    }

    // Returns the report datapoint status for given date
    function typeForDate(d) {
        var type = "";
        var match = chart.data().find(function (element, index) {
            return matchDates(element, d);
        });
        if (match) {
            type = match.type;
        }
        return type;
    }

    // Method for matching dates according to frequency
    function matchDates(dataDate, timelineDate) {
        if (frequency === 'hour') {
            return moment.utc(dataDate.date).isSame(timelineDate, 'day') && moment.utc(dataDate.date).isSame(timelineDate, 'hour');
        } else if (frequency === 'day') {
            return moment.utc(dataDate.date).isSame(timelineDate, 'day');
        } else if (frequency === 'week') {
            return moment.utc(dataDate.date).subtract(2, 'day').isSame(timelineDate, 'week');
        } else if (frequency === 'month') {
            return moment.utc(dataDate.date).isSame(timelineDate, 'month') && moment.utc(dataDate.date).isSame(timelineDate, 'year');
        }
    }

    function getDateTimestamp(d) {
	    var timestamp = undefined;
	    var match = chart.data().find(function (element, index) {
		    return matchDates(element, d);
	    });
	    if (match) {
		    timestamp = match.timestamp;
	    }
	    return timestamp;
    }

    // Identifies the weekday index in case of weekstart = 1 or 0
    function formatWeekday(d) {
        var weekDay = moment.utc(d).isoWeekday() % 7;
        if (weekStart === 1) {
            if (weekDay === 0) {
                return 6;
            } else {
                return weekDay - 1;
            }
        }
        return weekDay;
    }

    return chart;
}