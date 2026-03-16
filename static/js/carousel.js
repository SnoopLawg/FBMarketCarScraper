/* ── Image Carousel ── */
function carouselImgError(img) {
    const carousel = img.closest('.carousel');
    const fallback = carousel && carousel.dataset.fallback;
    if (fallback && img.src !== fallback && !img.dataset.triedFallback) {
        img.dataset.triedFallback = '1';
        img.src = fallback;
    } else {
        img.style.display = 'none';
        if (img.classList.contains('active') && carousel) {
            const wrapper = carousel.closest('.deal-img-wrapper');
            if (wrapper && !wrapper.querySelector('.deal-img-empty')) {
                carousel.style.display = 'none';
                const ph = document.createElement('div');
                ph.className = 'deal-img-empty';
                ph.textContent = 'No image';
                wrapper.appendChild(ph);
            }
        }
    }
}

function carouselNav(btn, dir) {
    const carousel = btn.closest('.carousel');
    const slides = carousel.querySelectorAll('.carousel-slide');
    const dots = carousel.querySelectorAll('.carousel-dot');
    let idx = parseInt(carousel.dataset.index || 0) + dir;
    idx = (idx + slides.length) % slides.length;
    carousel.dataset.index = idx;
    slides.forEach((s, i) => {
        s.classList.toggle('active', i === idx);
        if (i === idx && !s.src && s.dataset.src) s.src = s.dataset.src;
    });
    dots.forEach((d, i) => d.classList.toggle('active', i === idx));
}
