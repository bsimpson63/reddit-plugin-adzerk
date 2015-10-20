(function(global, undefined) {
  'use strict';

  global.ados_results = global.ados_results || null;

  var NETWORK = global.ADS_GLOBALS.network;
  var SITE = global.ADS_GLOBALS.site;
  var PLACEMENT_TYPES = {
    main: 5,
    sponsorship: 8,
  };

  function getConfig() {
    // Accessing `location.hash` directly does different
    // things in different browsers:
    //    > location.hash = "#%30";
    //    > location.hash === "#0"; // This is wrong, it should be "#%30"
    //    > true 
    // see http://stackoverflow.com/a/1704842/704286
    var hash = location.href.split('#')[1] || '';

    // Firefox automatically encodes thing the fragment, but not other browsers.
    if (/^\{%22/.test(hash)) {
      hash = decodeURIComponent((hash));
    }

    try {
      return $.parseJSON(hash);
    } catch (e) {
      return {};
    }
  }

  var config = getConfig();

  ados.run.push(function() {
    ados.isAsync = true;

    if (config.placements) {
      var placements = config.placements.split(',');

      for (var i = 0; i < placements.length; i++) {
        var kvp = placements[i].split(':');
        var type = kvp[0];
        var creative = kvp[1];

        ados_add_placement(NETWORK, SITE, type, PLACEMENT_TYPES[type])
          .setFlightCreativeId(creative);
      }
    } else {
      for (var type in PLACEMENT_TYPES) {
        ados_add_placement(NETWORK, SITE, type, PLACEMENT_TYPES[type]);
      }
    }
    
    ados_setWriteResults(true);

    if (config.keywords) {
      ados_setKeywords(config.keywords);
    }

    ados_load();

    var timeout = 0;
    var load = setInterval(function() {
      timeout++;
      if (global.ados_results) {
        clearInterval(load);

        // Load companion
        if (global.ados_results.sponsorship) {
          if (global.postMessage) {
            global.parent.postMessage('ados.createAdFrame:sponsorship', config.origin);
          } else {
            iframe = document.createElement('iframe');
            iframe.src = '/static/createadframe.html';
            iframe.style.display = 'none';
            document.documentElement.appendChild(iframe);
          }
        }
      }
    }, 50);
  });
})(this);
