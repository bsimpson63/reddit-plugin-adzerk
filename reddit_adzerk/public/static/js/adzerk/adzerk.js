r.adzerk = {
    origin: location.protocol == 'https:'
            ? 'https://az.turbobytes.net'
            : 'http://static.adzerk.net',

    createSponsorshipAdFrame: function() {
        var iframe = $('<iframe>')
            .attr({
                'id': 'ad_sponsorship',
                'src': r.adzerk.origin + '/reddit/ads-load.html?bust2',
                'frameBorder': 0,
                'scrolling': 'no'
            })
        $('.side .sponsorshipbox')
            .empty()
            .append(iframe)
    }
}

$(window).on('message', function(ev) {
    ev = ev.originalEvent
    if (ev.origin != r.adzerk.origin) {
      return
    }
    msg = ev.data.split(':')
    if (msg[0] == 'ados.createAdFrame') {
      r.adzerk.createSponsorshipAdFrame()
    }
})
