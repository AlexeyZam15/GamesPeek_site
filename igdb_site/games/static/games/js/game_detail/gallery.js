// games/static/games/js/game_detail/gallery.js

console.log('🎯 GALLERY SCRIPT LOADED');

let galleryImages = [];
let currentIndex = 0;
let modalElement = null;
let modalImage = null;
let isCardGallery = false;
let cardImages = [];
let isNavigating = false; // Флаг для предотвращения дублирования

// Константы для масштабирования
const QUALITY_THRESHOLDS = {
    MINIMAL: { width: 800, height: 450 }
};
const MAX_SCALE = 1.8;

// Кеш для размеров экрана
let cachedScreenWidth = window.innerWidth;
let cachedScreenHeight = window.innerHeight;

// Функция масштабирования
function scaleImage(img) {
    if (!img || !img.src) return;

    const tempImg = new Image();
    tempImg.onload = function() {
        const originalWidth = tempImg.width;
        const originalHeight = tempImg.height;

        const screenWidth = cachedScreenWidth;
        const screenHeight = cachedScreenHeight;
        const maxWidth = screenWidth * 0.9;
        const maxHeight = screenHeight * 0.85;

        let scale = 1.0;

        if (originalWidth < QUALITY_THRESHOLDS.MINIMAL.width ||
            originalHeight < QUALITY_THRESHOLDS.MINIMAL.height) {
            scale = Math.min(MAX_SCALE, Math.max(
                QUALITY_THRESHOLDS.MINIMAL.width / originalWidth,
                QUALITY_THRESHOLDS.MINIMAL.height / originalHeight
            ));
        }

        img.style.transform = scale > 1.05 ? `scale(${scale})` : '';
        img.style.transformOrigin = 'center center';
        img.style.maxWidth = `${maxWidth}px`;
        img.style.maxHeight = `${maxHeight}px`;
        img.style.objectFit = 'contain';
    };
    tempImg.src = img.src;
}

// Обновление изображения в модалке
function updateModalImage(src, alt, index, total) {
    if (!modalImage) return;

    modalImage.src = src;
    modalImage.alt = alt || 'Screenshot';

    const currentSpan = document.getElementById('currentNum');
    const totalSpan = document.getElementById('totalCount');

    if (currentSpan) currentSpan.textContent = index + 1;
    if (totalSpan && total) totalSpan.textContent = ` / ${total}`;

    scaleImage(modalImage);
}

// Открытие галереи из карточек
window.openGalleryFromCard = function(images, startIndex) {
    if (!modalElement) return;

    cardImages = images;
    isCardGallery = true;
    currentIndex = startIndex || 0;

    const img = cardImages[currentIndex];
    updateModalImage(img.url, img.alt, currentIndex, cardImages.length);

    const bsModal = bootstrap.Modal.getInstance(modalElement) || new bootstrap.Modal(modalElement);
    bsModal.show();
};

// Закрытие галереи
if (modalElement) {
    modalElement.addEventListener('hidden.bs.modal', function() {
        isCardGallery = false;
        cardImages = [];
        isNavigating = false;
    });
}

// Навигация с зацикливанием (с защитой от дублирования)
function onNavClick(direction) {
    if (isNavigating) return;
    isNavigating = true;

    let newIndex = currentIndex;

    if (isCardGallery && cardImages.length) {
        if (direction === 'prev') {
            newIndex = currentIndex - 1;
            if (newIndex < 0) newIndex = cardImages.length - 1;
        } else {
            newIndex = currentIndex + 1;
            if (newIndex >= cardImages.length) newIndex = 0;
        }

        currentIndex = newIndex;
        updateModalImage(cardImages[currentIndex].url, cardImages[currentIndex].alt, currentIndex, cardImages.length);

    } else if (galleryImages.length) {
        if (direction === 'prev') {
            newIndex = currentIndex - 1;
            if (newIndex < 0) newIndex = galleryImages.length - 1;
        } else {
            newIndex = currentIndex + 1;
            if (newIndex >= galleryImages.length) newIndex = 0;
        }

        currentIndex = newIndex;
        const img = galleryImages[currentIndex];
        const src = img.getAttribute('data-image') || img.src;
        const alt = img.alt || 'Screenshot';
        updateModalImage(src, alt, currentIndex, galleryImages.length);
    }

    setTimeout(() => {
        isNavigating = false;
    }, 300);
}

// Обработчики событий
function bindEvents() {
    const prevBtn = document.getElementById('prevBtn');
    const nextBtn = document.getElementById('nextBtn');

    if (prevBtn) prevBtn.onclick = () => onNavClick('prev');
    if (nextBtn) nextBtn.onclick = () => onNavClick('next');

    // 1. СВАЙПЫ для мобильных
    let touchStartX = 0;
    let touchEndX = 0;
    let isSwiping = false;

    function handleSwipe() {
        if (isSwiping) return;
        const diff = touchEndX - touchStartX;
        if (Math.abs(diff) > 50) {
            isSwiping = true;
            if (diff > 0) {
                onNavClick('prev');
            } else {
                onNavClick('next');
            }
            setTimeout(() => {
                isSwiping = false;
            }, 300);
        }
    }

    if (modalImage) {
        modalImage.addEventListener('touchstart', function(e) {
            touchStartX = e.touches[0].clientX;
        });

        modalImage.addEventListener('touchend', function(e) {
            touchEndX = e.changedTouches[0].clientX;
            handleSwipe();
        });
    }

    // 2. КЛИК ПО КРАЯМ ЭКРАНА (15% слева и справа)
    if (modalElement) {
        modalElement.addEventListener('click', function(e) {
            // Не срабатываем если клик по кнопкам
            if (e.target.closest('#prevBtn') || e.target.closest('#nextBtn') || e.target.closest('.screenshot-close')) {
                return;
            }

            // Не срабатываем если блокировка
            if (isNavigating) return;

            const screenWidth = window.innerWidth;
            const clickX = e.clientX;
            const edgeZone = screenWidth * 0.15;

            if (clickX < edgeZone) {
                onNavClick('prev');
            } else if (clickX > (screenWidth - edgeZone)) {
                onNavClick('next');
            }
        });
    }

    window.addEventListener('resize', function() {
        cachedScreenWidth = window.innerWidth;
        cachedScreenHeight = window.innerHeight;
        if (modalElement?.classList.contains('show') && modalImage?.src) {
            scaleImage(modalImage);
        }
    });
}

// Инициализация галереи на странице
window.initInlineGallery = function() {
    modalElement = document.getElementById('screenshotModal');
    modalImage = document.getElementById('modalImage');

    if (!modalElement) return;

    galleryImages = Array.from(document.querySelectorAll(
        '.game-carousel-cover, .game-carousel-screenshot, .screenshot-image-fixed, .gallery-img, img[data-image]'
    ));

    if (!galleryImages.length) return;

    if (!window._galleryClickHandler) {
        window._galleryClickHandler = function(e) {
            const img = e.target.closest('.game-carousel-cover, .game-carousel-screenshot, .screenshot-image-fixed, .gallery-img, img[data-image]');
            if (!img || img.closest('#screenshotModal')) return;

            e.preventDefault();
            e.stopPropagation();

            const src = img.getAttribute('data-image') || img.src;
            const alt = img.alt || 'Screenshot';
            const index = galleryImages.findIndex(i => (i.getAttribute('data-image') || i.src) === src);

            if (index !== -1) {
                currentIndex = index;
                isCardGallery = false;
                updateModalImage(src, alt, currentIndex, galleryImages.length);

                const modal = bootstrap.Modal.getInstance(modalElement) || new bootstrap.Modal(modalElement);
                modal.show();
            }
        };
        document.body.addEventListener('click', window._galleryClickHandler);
    }

    bindEvents();

    console.log(`✅ Gallery initialized with ${galleryImages.length} images`);
};

// Инициализация
document.addEventListener('DOMContentLoaded', function() {
    window.initInlineGallery();
});

console.log('✅ Gallery script ready');