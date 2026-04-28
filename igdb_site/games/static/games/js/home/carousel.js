// games/static/games/js/home/carousel.js
// Home page carousel functionality for GamesPeek

function moveCarousel(trackId, direction) {
    const track = document.getElementById(trackId);
    if (!track) return;

    const slides = track.children;
    if (slides.length === 0) return;

    const slideWidth = slides[0]?.offsetWidth || 0;
    const gap = 24;
    const visibleSlides = window.innerWidth <= 576 ? 1 : window.innerWidth <= 992 ? 2 : 4;
    const scrollAmount = (slideWidth + gap) * visibleSlides;

    let currentTransform = track.style.transform;
    let currentTranslate = 0;
    if (currentTransform) {
        const match = currentTransform.match(/translateX\(([-\d.]+)px\)/);
        if (match) currentTranslate = parseFloat(match[1]);
    }

    let newTranslate = currentTranslate - (direction * scrollAmount);
    const maxTranslate = 0;
    const minTranslate = -((slides.length - visibleSlides) * (slideWidth + gap));
    newTranslate = Math.max(minTranslate, Math.min(maxTranslate, newTranslate));

    track.style.transform = `translateX(${newTranslate}px)`;

    const currentIndex = Math.abs(Math.round(newTranslate / (slideWidth + gap)));
    const indicatorsId = trackId.replace('CarouselTrack', 'CarouselIndicators');
    updateIndicators(indicatorsId, currentIndex, slides.length);
}

function updateIndicators(indicatorsId, currentIndex, totalSlides) {
    const indicators = document.getElementById(indicatorsId);
    if (!indicators) return;

    indicators.innerHTML = '';
    for (let i = 0; i < totalSlides; i++) {
        const dot = document.createElement('span');
        dot.className = `carousel-dot ${i === currentIndex ? 'active' : ''}`;
        dot.onclick = (function(idx) {
            return function() {
                const trackId = indicatorsId.replace('CarouselIndicators', 'CarouselTrack');
                goToSlide(trackId, idx);
            };
        })(i);
        indicators.appendChild(dot);
    }
}

function goToSlide(trackId, slideIndex) {
    const track = document.getElementById(trackId);
    if (!track) return;

    const slides = track.children;
    if (slides.length === 0) return;

    const slideWidth = slides[0]?.offsetWidth || 0;
    const gap = 24;
    const visibleSlides = window.innerWidth <= 576 ? 1 : window.innerWidth <= 992 ? 2 : 4;
    const maxIndex = slides.length - visibleSlides;
    const targetIndex = Math.max(0, Math.min(slideIndex, maxIndex));
    const newTranslate = -(targetIndex * (slideWidth + gap));

    track.style.transform = `translateX(${newTranslate}px)`;
    const indicatorsId = trackId.replace('CarouselTrack', 'CarouselIndicators');
    updateIndicators(indicatorsId, targetIndex, slides.length);
}

function initHomeCarousels() {
    const popularTrack = document.getElementById('popularCarouselTrack');
    const recentReleaseTrack = document.getElementById('recentReleaseCarouselTrack');
    const recentlyAddedTrack = document.getElementById('recentlyAddedCarouselTrack');

    if (popularTrack && popularTrack.children.length > 0) {
        updateIndicators('popularCarouselIndicators', 0, popularTrack.children.length);
    }
    if (recentReleaseTrack && recentReleaseTrack.children.length > 0) {
        updateIndicators('recentReleaseCarouselIndicators', 0, recentReleaseTrack.children.length);
    }
    if (recentlyAddedTrack && recentlyAddedTrack.children.length > 0) {
        updateIndicators('recentlyAddedCarouselIndicators', 0, recentlyAddedTrack.children.length);
    }
}

document.addEventListener('DOMContentLoaded', initHomeCarousels);

window.addEventListener('resize', function() {
    initHomeCarousels();
    const popularTrack = document.getElementById('popularCarouselTrack');
    const recentReleaseTrack = document.getElementById('recentReleaseCarouselTrack');
    const recentlyAddedTrack = document.getElementById('recentlyAddedCarouselTrack');
    if (popularTrack) goToSlide('popularCarouselTrack', 0);
    if (recentReleaseTrack) goToSlide('recentReleaseCarouselTrack', 0);
    if (recentlyAddedTrack) goToSlide('recentlyAddedCarouselTrack', 0);
});