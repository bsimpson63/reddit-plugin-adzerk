!(function(window, $, undefined) {
  'use strict';

  window.r = window.r || {};

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

  $(window).on('message', function(e) {
    e = e.originalEvent;

    if (!new RegExp('^http(s)?:\\/\\/' + r.config.media_domain, 'i').test(e.origin)) {
      return;
    }

    var messsage = e.data.split(':')
    if (messsage[0] == 'ados.createAdFrame') {
      r.adzerk.createSponsorshipAdFrame();
    }
  });

})(this, this.jQuery);
