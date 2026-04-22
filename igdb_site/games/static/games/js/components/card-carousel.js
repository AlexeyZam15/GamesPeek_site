// games/static/games/js/components/card-carousel.js

const CardCarousel = {
    initialized: false,
    carousels: new Map(),
    galleryModal: null,
    galleryCarousel: null,

    init: function() {
        if (this.initialized) return;

        console.log('CardCarousel: Initializing carousels for all game cards');

        this.initAllCarousels();
        this.setupEventListeners();
        this.createGalleryModal();

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

        const gameId = gameCard.dataset.gameId;
        const images = this.collectImagesFromCard(gameCard);

        if (images.length === 0) return;

        this.openGallery(images, clickedIndex, gameId);
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

    createGalleryModal: function() {
        let modal = document.getElementById('cardGalleryModal');

        if (modal) {
            modal.remove();
        }

        modal = document.createElement('div');
        modal.id = 'cardGalleryModal';
        modal.className = 'modal fade';
        modal.setAttribute('tabindex', '-1');
        modal.setAttribute('aria-hidden', 'true');

        modal.innerHTML = `
            <div class="modal-dialog modal-fullscreen">
                <div class="modal-content bg-black">
                    <div class="modal-header border-0 position-absolute top-0 end-0 z-3">
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body d-flex align-items-center justify-content-center p-0">
                        <div id="cardGalleryCarousel" class="carousel slide w-100 h-100" data-bs-ride="carousel" data-bs-interval="false">
                            <div class="carousel-inner h-100 d-flex align-items-center" id="cardGalleryInner"></div>
                            <button class="carousel-control-prev" type="button" data-bs-target="#cardGalleryCarousel" data-bs-slide="prev">
                                <span class="carousel-control-prev-icon" aria-hidden="true"></span>
                                <span class="visually-hidden">Previous</span>
                            </button>
                            <button class="carousel-control-next" type="button" data-bs-target="#cardGalleryCarousel" data-bs-slide="next">
                                <span class="carousel-control-next-icon" aria-hidden="true"></span>
                                <span class="visually-hidden">Next</span>
                            </button>
                            <div class="carousel-indicators" id="cardGalleryIndicators"></div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        document.body.appendChild(modal);

        this.galleryModal = modal;
        this.galleryCarousel = document.getElementById('cardGalleryCarousel');

        modal.addEventListener('hidden.bs.modal', () => {
            this.stopCarouselRotation();
        });
    },

    openGallery: function(images, startIndex, gameId) {
        if (!this.galleryModal || !this.galleryCarousel) {
            this.createGalleryModal();
        }

        this.populateGalleryCarousel(images, startIndex);

        const modalInstance = bootstrap.Modal.getInstance(this.galleryModal);
        if (modalInstance) {
            modalInstance.show();
        } else {
            const newModal = new bootstrap.Modal(this.galleryModal);
            newModal.show();
        }

        setTimeout(() => {
            this.syncCarouselWithStartIndex(startIndex);
        }, 100);
    },

    populateGalleryCarousel: function(images, startIndex) {
        const inner = document.getElementById('cardGalleryInner');
        const indicators = document.getElementById('cardGalleryIndicators');

        if (!inner || !indicators) return;

        inner.innerHTML = '';
        indicators.innerHTML = '';

        images.forEach((image, idx) => {
            const isActive = idx === startIndex;

            const item = document.createElement('div');
            item.className = `carousel-item ${isActive ? 'active' : ''}`;
            item.innerHTML = `
                <img src="${image.url}" alt="${image.alt}" class="d-block mx-auto" style="max-width: 95vw; max-height: 80vh; object-fit: contain;">
            `;
            inner.appendChild(item);

            const indicator = document.createElement('button');
            indicator.type = 'button';
            indicator.setAttribute('data-bs-target', '#cardGalleryCarousel');
            indicator.setAttribute('data-bs-slide-to', idx);
            if (isActive) {
                indicator.className = 'active';
                indicator.setAttribute('aria-current', 'true');
            }
            indicator.setAttribute('aria-label', `Slide ${idx + 1}`);
            indicators.appendChild(indicator);
        });

        const bsCarousel = new bootstrap.Carousel(this.galleryCarousel, {
            interval: false,
            wrap: true,
            touch: true
        });

        this.galleryCarousel.bsCarousel = bsCarousel;
    },

    syncCarouselWithStartIndex: function(startIndex) {
        if (this.galleryCarousel && this.galleryCarousel.bsCarousel) {
            this.galleryCarousel.bsCarousel.to(startIndex);
        }
    },

    stopCarouselRotation: function() {
        if (this.galleryCarousel && this.galleryCarousel.bsCarousel) {
            this.galleryCarousel.bsCarousel.pause();
        }
    },

    setupEventListeners: function() {
        document.addEventListener('games-grid-updated', () => {
            setTimeout(() => {
                this.reinitCarousels();
            }, 200);
        });

        document.addEventListener('ajax-content-loaded', () => {
            setTimeout(() => {
                this.reinitCarousels();
            }, 200);
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
                setTimeout(() => {
                    this.reinitCarousels();
                }, 100);
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