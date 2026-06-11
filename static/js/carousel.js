/* ── Image Carousel ──
   carouselImgError() is defined early in base.html <head> so broken-image
   onerror handlers resolve during HTML parse (before this file loads). */

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
