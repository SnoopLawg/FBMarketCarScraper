/* ── Deal Card Actions ── */

/* Favorite toggle */
async function toggleFav(btn, href) {
    const res = await fetch('/api/favorite', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({href})
    });
    const data = await res.json();
    if (data.saved) {
        const card = btn.closest('.deal-card');
        if (card) {
            card.style.transition = 'opacity 0.3s, transform 0.3s';
            card.style.opacity = '0';
            card.style.transform = 'scale(0.95)';
            setTimeout(() => card.remove(), 300);
        }
    } else {
        btn.classList.remove('active');
    }
}

/* Delete */
async function deleteDeal(href, cardId) {
    await fetch('/api/delete', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({href})
    });
    const card = document.getElementById(cardId);
    card.style.transition = 'opacity 0.3s, transform 0.3s';
    card.style.opacity = '0';
    card.style.transform = 'scale(0.95)';
    setTimeout(() => card.remove(), 300);
}

/* VIN Decode */
async function decodeVin(vin, btn) {
    btn.disabled = true;
    btn.textContent = '...';
    try {
        const res = await fetch('/api/vin-decode/' + vin);
        if (res.ok) {
            const data = await res.json();
            const parts = [data.year, data.make, data.model, data.trim].filter(Boolean);
            const extra = [data.drive_type, data.fuel_type].filter(Boolean).join(' \u00b7 ');
            btn.outerHTML = '<span class="badge badge-info" style="margin-left:4px;font-size:10px">Verified</span>';
            const vinLine = btn.closest ? btn.closest('.deal-detail') : btn.parentElement;
            if (vinLine && parts.length) {
                const info = document.createElement('div');
                info.className = 'deal-detail deal-detail-muted';
                info.style.cssText = 'padding-left:22px;font-size:11px';
                info.textContent = parts.join(' ') + (extra ? ' \u00b7 ' + extra : '');
                vinLine.insertAdjacentElement('afterend', info);
            }
        } else {
            btn.textContent = 'N/A';
            btn.style.color = 'var(--red)';
        }
    } catch (e) {
        btn.textContent = 'Error';
        btn.style.color = 'var(--red)';
    }
}

/* Show more / Show less toggle */
document.addEventListener('click', function(e) {
    const toggle = e.target.closest('.deal-details-toggle');
    if (!toggle) return;
    const card = toggle.closest('.deal-card');
    const label = toggle.querySelector('span');
    if (label) {
        label.textContent = card.classList.contains('expanded') ? 'Show less' : 'Show more';
    }
    if (typeof lucide !== 'undefined') lucide.createIcons();
});

/* Track Car (Discover page) */
async function trackCar(carName, btn) {
    btn.disabled = true;
    btn.textContent = 'Tracking...';
    try {
        const res = await fetch('/api/track-car', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({car_name: carName})
        });
        const data = await res.json();
        if (data.ok) {
            btn.textContent = 'Tracked!';
            btn.style.color = 'var(--green)';
            btn.style.borderColor = 'var(--green)';
        } else {
            btn.textContent = 'Error';
            btn.disabled = false;
        }
    } catch (e) {
        btn.textContent = 'Error';
        btn.disabled = false;
    }
}
