r.adzerk = {
    origin: location.protocol == 'https:'
            ? 'https://az.turbobytes.net'
            : 'http://static.adzerk.net',

    createAdFrame: function(name) {
        if (name == 'sponsorship') {
            var iframe = $('<iframe>')
                .attr({
                    'id': 'ad_' + name,
                    'src': r.adzerk.origin + '/reddit/ads-load.html#sponsorship',
                    'frameBorder': 0,
                    'scrolling': 'no'
                })
            $('.side .sponsorshipbox')
                .empty()
                .append(iframe)
        }
    }
}

$(window).on('message', function(ev) {
    ev = ev.originalEvent
    if (ev.origin != r.adzerk.origin) {
      return
    }
    msg = ev.data.split(':')
    if (msg[0] == 'ados.createAdFrame') {
      r.adzerk.createAdFrame(msg[1])
    }
})
