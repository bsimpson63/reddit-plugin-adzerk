!(function(global, undefined) {
  'use strict';
  var PLACEMENT = 'sponsorship';

  global.onload = function() {
    var adContent = global.parent.frames.ad_main.ados_results;
    global.name = 'ad-' + PLACEMENT;

    // Grabs data from the main frame and `eval`s it. Unfortunately
    // there isn't a great way to pass this data across frames
    // as it's more complicated than json and would normally be
    // loaded as a script tag, hence the `eval`
    eval(adContent[PLACEMENT]);
  };

  ados.run.push(function() {
    ados_loadDiv(PLACEMENT);
  });
})(this);
