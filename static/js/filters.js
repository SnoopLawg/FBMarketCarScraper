/* ── Filter & Sort Logic ── */

function applyFilter(key, value) {
    const params = new URLSearchParams(window.location.search);
    if (value) params.set(key, value);
    else params.delete(key);
    window.location.search = params.toString();
}

function applyAllFilters() {
    const params = new URLSearchParams(window.location.search);
    const fields = {
        'q': 'search-input',
        'year_min': 'year-min', 'year_max': 'year-max',
        'mileage_min': 'mileage-min', 'mileage_max': 'mileage-max',
        'price_min': 'price-min', 'price_max': 'price-max',
    };
    for (const [param, elId] of Object.entries(fields)) {
        const el = document.getElementById(elId);
        if (!el) continue;
        const val = el.value;
        if (val) params.set(param, val); else params.delete(param);
    }
    window.location.search = params.toString();
}

function clearFilters() {
    // Use data attribute or default to current path
    const clearUrl = document.getElementById('filter-row')?.dataset.clearUrl || window.location.pathname;
    window.location.href = clearUrl;
}

function toggleMobileFilters() {
    document.getElementById('filter-row').classList.toggle('open');
}

// Bind numeric filter inputs to auto-apply on change
document.addEventListener('DOMContentLoaded', function() {
    ['year-min','year-max','mileage-min','mileage-max','price-min','price-max'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('change', applyAllFilters);
    });
});
