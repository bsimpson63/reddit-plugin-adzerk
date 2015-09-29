(function(global, undefined) {
  'use strict';

  global.ados_results = global.ados_results || null;

  var NETWORK = global.ADS_GLOBALS.network;
  var SITE = global.ADS_GLOBALS.site;
  var PLACEMENT_TYPES = {
    main: 5,
    sponsorship: 8,
  };
  
  function parseQs(search) {
    search = search.replace(/^\?/, '');

    var kvps = search.split('&');
    var result = {};

    for (var i = 0; i < kvps.length; i++) {
      var kvp = kvps[i].split('=');
      var key = kvp[0];
      var value = kvp[1];

      result[key] = decodeURIComponent(value);
    }

    return result;
  }

  var query = parseQs(location.search);

  ados.run.push(function() {
    ados.isAsync = true;

    if (query.placements) {
      var placements = query.placements.split(',');

      for (var i = 0; i < placements.length; i++) {
        var kvp = placements[i].split(':');
        var type = kvp[0];
        var creative = kvp[1];

        ados_add_placement(NETWORK, SITE, type, PLACEMENT_TYPES[type])
          .setFlightCreativeId(creative);
      };
    } else {
      for (var type in PLACEMENT_TYPES) {
        ados_add_placement(NETWORK, SITE, type, PLACEMENT_TYPES[type]);
      }
    }
    
    ados_setWriteResults(true);

    if (query.sr) {
      ados_setKeywords(query.sr);
    }

    ados_load();

    var target = location.hash.substr(1);
    var timeout = 0;
    var load = setInterval(function() {
      timeout++;
      if (global.ados_results) {
        clearInterval(load);

        // Load companion
        if (global.ados_results.sponsorship) {
          if (global.postMessage) {
            global.parent.postMessage('ados.createAdFrame:sponsorship', target);
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
