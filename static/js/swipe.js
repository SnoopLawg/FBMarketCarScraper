/* ── Touch Swipe (mobile: left=delete, right=favorite) ── */
(function() {
    if (!('ontouchstart' in window)) return;

    let startX, startY, currentCard, swiping = false;
    const THRESHOLD = 100;

    document.addEventListener('touchstart', function(e) {
        const card = e.target.closest('.deal-card');
        if (!card) return;
        const carousel = e.target.closest('.carousel');
        if (carousel && carousel.querySelectorAll('.carousel-slide').length > 1) return;

        startX = e.touches[0].clientX;
        startY = e.touches[0].clientY;
        currentCard = card;
        swiping = false;
    }, {passive: true});

    document.addEventListener('touchmove', function(e) {
        if (!currentCard) return;
        const dx = e.touches[0].clientX - startX;
        const dy = e.touches[0].clientY - startY;

        if (!swiping && Math.abs(dx) > 15 && Math.abs(dx) > Math.abs(dy) * 1.5) {
            swiping = true;
            currentCard.classList.add('swiping');
        }
        if (!swiping) return;

        e.preventDefault();
        const rotate = dx * 0.04;
        currentCard.style.transform = `translateX(${dx}px) rotate(${rotate}deg)`;

        const favInd = currentCard.querySelector('.swipe-fav');
        const delInd = currentCard.querySelector('.swipe-delete');
        if (favInd) favInd.style.opacity = dx > 30 ? Math.min(1, (dx - 30) / 80) : 0;
        if (delInd) delInd.style.opacity = dx < -30 ? Math.min(1, (-dx - 30) / 80) : 0;
    }, {passive: false});

    document.addEventListener('touchend', function(e) {
        if (!currentCard || !swiping) { currentCard = null; return; }
        const dx = e.changedTouches[0].clientX - startX;
        const card = currentCard;
        currentCard = null;

        const favInd = card.querySelector('.swipe-fav');
        const delInd = card.querySelector('.swipe-delete');

        if (dx > THRESHOLD) {
            const href = card.dataset.href;
            fetch('/api/favorite', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({href})
            });
            card.style.transition = 'transform 0.3s ease, opacity 0.3s ease';
            card.style.transform = 'translateX(120%) rotate(8deg)';
            card.style.opacity = '0';
            setTimeout(() => card.remove(), 300);
        } else if (dx < -THRESHOLD) {
            const href = card.dataset.href;
            fetch('/api/delete', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({href})
            });
            card.style.transition = 'transform 0.3s ease, opacity 0.3s ease';
            card.style.transform = 'translateX(-120%) rotate(-8deg)';
            card.style.opacity = '0';
            setTimeout(() => card.remove(), 300);
        } else {
            card.style.transition = 'transform 0.25s ease';
            card.style.transform = 'translateX(0)';
            if (favInd) favInd.style.opacity = 0;
            if (delInd) delInd.style.opacity = 0;
            setTimeout(() => { card.classList.remove('swiping'); card.style.transition = ''; }, 250);
        }
    }, {passive: true});
})();
