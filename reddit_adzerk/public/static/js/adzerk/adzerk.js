!(function(global, $, undefined) {
  'use strict';

  global.r = global.r || {};

  r.adzerk = {

    createSponsorshipAdFrame: function(overrideSrc) {
      var $iframe = $('<iframe>');

      $iframe
        .attr({
            id: 'ad_sponsorship',
            src: '//' + r.config.media_domain + '/ads/display/300x250-companion',
            frameBorder: 0,
            scrolling: 'no',
        });

      $('.side .sponsorshipbox')
        .empty()
        .append($iframe);
    },

  };

  $(global).on('message', function(e) {
    e = e.originalEvent;

    if (!new RegExp('^http(s)?:\\/\\/' + r.config.media_domain, 'i').test(e.origin)) {
      return;
    }

    var data = e.data;

    if (typeof data === 'string') {
      var message = data.split(':');

      if (message[0] == 'ados.createAdFrame') {
        r.adzerk.createSponsorshipAdFrame();
      }
    }

    if (window.frames.ad_main && window.frames.ad_main.postMessage) {
      window.frames.ad_main.postMessage(data, '*');
    }
  });

})(this, this.jQuery);
