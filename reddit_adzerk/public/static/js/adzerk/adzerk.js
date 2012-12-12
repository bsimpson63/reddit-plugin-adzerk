r.adzerk = {
    origin: 'http://static.adzerk.net',
    createAdFrame: function(name) {
        if (name == 'sponsorship') {
            var iframe = $('<iframe>')
                .attr({
                    'id': 'ad-' + name,
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
    if (ev.origin != r.adzerk.origin) {
      return
    }
    msg = ev.data.split(':')
    if (msg[0] == 'ados.createAdFrame') {
      createAdFrame(msg[1])
    }
})
