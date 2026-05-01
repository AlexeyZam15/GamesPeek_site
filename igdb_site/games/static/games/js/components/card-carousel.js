// games/static/games/js/components/card-carousel.js

const CardCarousel = {
    initialized: false,
    carousels: new Map(),

    init: function() {
        if (this.initialized) return;

        console.log('CardCarousel: Initializing carousels for all game cards');

        this.initAllCarousels();
        this.setupEventListeners();

        this.initialized = true;
    },

    initAllCarousels: function() {
        const carousels = document.querySelectorAll('.game-screenshot-carousel');
        carousels.forEach(carousel => {
            this.initSingleCarousel(carousel);
        });
        console.log(`CardCarousel: Initialized ${carousels.length} carousels`);
    },

    initSingleCarousel: function(carouselElement) {
        const carouselId = carouselElement.id;
        if (!carouselId) return;

        if (this.carousels.has(carouselId)) {
            const existingCarousel = this.carousels.get(carouselId);
            try {
                existingCarousel.dispose();
            } catch(e) {}
        }

        try {
            const bsCarousel = new bootstrap.Carousel(carouselElement, {
                interval: false,
                wrap: true,
                touch: true,
                pause: 'hover'
            });
            this.carousels.set(carouselId, bsCarousel);
            this.setupGalleryOnClick(carouselElement);
        } catch (error) {
            console.error(`CardCarousel: Failed to init carousel ${carouselId}:`, error);
        }
    },

    setupGalleryOnClick: function(carouselElement) {
        const slides = carouselElement.querySelectorAll('.carousel-item img');
        slides.forEach((img, index) => {
            img.removeEventListener('click', this.handleImageClick);
            img.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.handleImageClick(e, carouselElement, index);
            });
        });
    },

    handleImageClick: function(event, carouselElement, clickedIndex) {
        const gameCard = carouselElement.closest('.game-card-container');
        if (!gameCard) return;

        const images = this.collectImagesFromCard(gameCard);
        if (images.length === 0) return;

        if (window.openGalleryFromCard) {
            window.openGalleryFromCard(images, clickedIndex);
        }
    },

    collectImagesFromCard: function(gameCard) {
        const images = [];
        const carousel = gameCard.querySelector('.game-screenshot-carousel');
        if (!carousel) return images;

        const slides = carousel.querySelectorAll('.carousel-item');
        slides.forEach((slide, idx) => {
            const img = slide.querySelector('img');
            if (img && img.src) {
                images.push({
                    url: img.src,
                    alt: img.alt || `Screenshot ${idx + 1}`,
                    type: idx === 0 ? 'cover' : 'screenshot'
                });
            }
        });
        return images;
    },

    setupEventListeners: function() {
        document.addEventListener('games-grid-updated', () => {
            setTimeout(() => this.reinitCarousels(), 200);
        });
        document.addEventListener('ajax-content-loaded', () => {
            setTimeout(() => this.reinitCarousels(), 200);
        });

        const observer = new MutationObserver((mutations) => {
            let shouldReinit = false;
            mutations.forEach(mutation => {
                if (mutation.addedNodes.length) {
                    mutation.addedNodes.forEach(node => {
                        if (node.nodeType === 1) {
                            if (node.classList && node.classList.contains('game-card-container')) {
                                shouldReinit = true;
                            }
                            if (node.querySelector && node.querySelector('.game-card-container')) {
                                shouldReinit = true;
                            }
                        }
                    });
                }
            });
            if (shouldReinit) {
                setTimeout(() => this.reinitCarousels(), 100);
            }
        });
        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    },

    reinitCarousels: function() {
        this.carousels.forEach((carousel, id) => {
            try {
                carousel.dispose();
            } catch(e) {}
        });
        this.carousels.clear();
        this.initAllCarousels();
    },

    refresh: function() {
        this.reinitCarousels();
    }
};

document.addEventListener('DOMContentLoaded', () => {
    CardCarousel.init();
});

window.CardCarousel = CardCarousel;