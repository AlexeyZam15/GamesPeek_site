// games/static/games/js/game_detail/gallery.js

console.log('🎯 GALLERY SCRIPT LOADED');

let galleryInitialized = false;
let galleryImages = [];
let currentImageIndex = 0;

// Константы для адаптивного масштабирования
const QUALITY_THRESHOLDS = {
    GOOD: { width: 1280, height: 720 },
    ACCEPTABLE: { width: 1024, height: 576 },
    MINIMAL: { width: 800, height: 450 }
};
const MAX_SCALE = 1.8;

// Функция масштабирования изображения в модальном окне
function scaleModalImage(img, originalWidth, originalHeight) {
    if (!img) return;

    const screenWidth = window.innerWidth;
    const screenHeight = window.innerHeight;
    const maxModalWidth = screenWidth * 0.9;
    const maxModalHeight = screenHeight * 0.85;

    let qualityScale = 1.0;

    if (originalWidth < QUALITY_THRESHOLDS.MINIMAL.width ||
        originalHeight < QUALITY_THRESHOLDS.MINIMAL.height) {
        const scaleToWidth = QUALITY_THRESHOLDS.MINIMAL.width / originalWidth;
        const scaleToHeight = QUALITY_THRESHOLDS.MINIMAL.height / originalHeight;
        qualityScale = Math.min(MAX_SCALE, Math.max(scaleToWidth, scaleToHeight));
    }
    else if (originalWidth < QUALITY_THRESHOLDS.ACCEPTABLE.width ||
             originalHeight < QUALITY_THRESHOLDS.ACCEPTABLE.height) {
        const scaleToWidth = QUALITY_THRESHOLDS.ACCEPTABLE.width / originalWidth;
        const scaleToHeight = QUALITY_THRESHOLDS.ACCEPTABLE.height / originalHeight;
        qualityScale = Math.min(MAX_SCALE, Math.max(scaleToWidth, scaleToHeight));
    }

    let targetWidth = originalWidth * qualityScale;
    let targetHeight = originalHeight * qualityScale;

    if (targetWidth > maxModalWidth) {
        const fitScale = maxModalWidth / targetWidth;
        targetWidth = maxModalWidth;
        targetHeight = targetHeight * fitScale;
    }

    if (targetHeight > maxModalHeight) {
        const fitScale = maxModalHeight / targetHeight;
        targetHeight = maxModalHeight;
        targetWidth = targetWidth * fitScale;
    }

    const finalScale = targetWidth / originalWidth;

    img.style.removeProperty('width');
    img.style.removeProperty('height');
    img.style.removeProperty('transform');
    img.style.removeProperty('transform-origin');
    img.style.removeProperty('max-width');
    img.style.removeProperty('max-height');

    if (finalScale > 1.05) {
        img.style.transform = `scale(${finalScale})`;
        img.style.transformOrigin = 'center center';
    }

    img.style.maxWidth = `${maxModalWidth}px`;
    img.style.maxHeight = `${maxModalHeight}px`;
    img.style.objectFit = 'contain';
    img.style.display = 'block';
    img.style.margin = 'auto';

    console.log(`✅ Scaled: ${originalWidth}x${originalHeight} → ${Math.round(targetWidth)}x${Math.round(targetHeight)}px (${finalScale.toFixed(2)}x)`);
}

// Функция для открытия галереи из карточек игр
window.openGalleryFromCard = function(images, startIndex) {
    const modal = document.getElementById('screenshotModal');
    if (!modal) {
        console.error('screenshotModal not found');
        return;
    }

    window.cardGalleryImages = images;
    window.cardGalleryCurrentIndex = startIndex || 0;

    const modalImg = document.getElementById('modalImage');
    const currentNumEl = document.getElementById('currentNum');
    const totalCountEl = document.getElementById('totalCount');

    if (totalCountEl) {
        totalCountEl.textContent = ` / ${images.length}`;
    }

    if (modalImg && images[window.cardGalleryCurrentIndex]) {
        modalImg.src = images[window.cardGalleryCurrentIndex].url;
        modalImg.alt = images[window.cardGalleryCurrentIndex].alt;

        const tempImg = new Image();
        tempImg.onload = function() {
            scaleModalImage(modalImg, tempImg.width, tempImg.height);
        };
        tempImg.src = images[window.cardGalleryCurrentIndex].url;
    }

    if (currentNumEl) {
        currentNumEl.textContent = window.cardGalleryCurrentIndex + 1;
    }

    const bsModal = bootstrap.Modal.getInstance(modal) || new bootstrap.Modal(modal);
    bsModal.show();
};

// Функция для открытия галереи из детальной страницы
function openScreenshotModal(imageUrl, altText, index, total) {
    const modal = document.getElementById('screenshotModal');
    if (!modal) return;

    const modalImg = document.getElementById('modalImage');
    const currentNumEl = document.getElementById('currentNum');
    const totalCountEl = document.getElementById('totalCount');

    if (totalCountEl && total) {
        totalCountEl.textContent = ` / ${total}`;
    }

    if (modalImg) {
        modalImg.src = imageUrl;
        modalImg.alt = altText || 'Screenshot';

        const tempImg = new Image();
        tempImg.onload = function() {
            scaleModalImage(modalImg, tempImg.width, tempImg.height);
        };
        tempImg.src = imageUrl;
    }

    if (currentNumEl && index !== undefined) {
        currentNumEl.textContent = index + 1;
    }

    currentImageIndex = index || 0;

    const bsModal = bootstrap.Modal.getInstance(modal) || new bootstrap.Modal(modal);
    bsModal.show();
}

// Инициализация галереи на детальной странице
function initDetailPageGallery() {
    const screenshotImages = document.querySelectorAll('.screenshot-image-fixed, .gallery-img');
    const total = screenshotImages.length;

    screenshotImages.forEach((img, idx) => {
        img.removeEventListener('click', detailClickHandler);
        img.addEventListener('click', detailClickHandler);

        function detailClickHandler(e) {
            e.preventDefault();
            e.stopPropagation();
            const url = img.getAttribute('data-image') || img.src;
            const alt = img.alt || 'Screenshot';
            openScreenshotModal(url, alt, idx, total);
        }
    });

    console.log(`✅ Detail page gallery initialized with ${total} screenshots`);
}

// Настройка обработчиков для модального окна
function setupModalHandlers() {
    const modal = document.getElementById('screenshotModal');
    if (!modal) return;

    const prevBtn = document.getElementById('prevBtn');
    const nextBtn = document.getElementById('nextBtn');

    if (prevBtn) {
        prevBtn.removeEventListener('click', handlePrevNext);
        prevBtn.addEventListener('click', handlePrevNext);
    }

    if (nextBtn) {
        nextBtn.removeEventListener('click', handlePrevNext);
        nextBtn.addEventListener('click', handlePrevNext);
    }

    function handlePrevNext(e) {
        e.preventDefault();
        e.stopPropagation();

        const isPrev = e.currentTarget.id === 'prevBtn';

        if (window.cardGalleryImages && window.cardGalleryImages.length > 0) {
            if (isPrev && window.cardGalleryCurrentIndex > 0) {
                window.cardGalleryCurrentIndex--;
            } else if (!isPrev && window.cardGalleryCurrentIndex < window.cardGalleryImages.length - 1) {
                window.cardGalleryCurrentIndex++;
            } else {
                return;
            }

            const modalImg = document.getElementById('modalImage');
            const currentNumEl = document.getElementById('currentNum');
            const newImage = window.cardGalleryImages[window.cardGalleryCurrentIndex];

            if (modalImg && newImage) {
                modalImg.src = newImage.url;
                modalImg.alt = newImage.alt;

                const tempImg = new Image();
                tempImg.onload = function() {
                    scaleModalImage(modalImg, tempImg.width, tempImg.height);
                };
                tempImg.src = newImage.url;
            }

            if (currentNumEl) {
                currentNumEl.textContent = window.cardGalleryCurrentIndex + 1;
            }
        } else if (galleryImages.length > 0) {
            if (isPrev && currentImageIndex > 0) {
                currentImageIndex--;
            } else if (!isPrev && currentImageIndex < galleryImages.length - 1) {
                currentImageIndex++;
            } else {
                return;
            }

            const modalImg = document.getElementById('modalImage');
            const currentNumEl = document.getElementById('currentNum');
            const newImage = galleryImages[currentImageIndex];

            if (modalImg && newImage) {
                const url = newImage.getAttribute('data-image') || newImage.src;
                modalImg.src = url;
                modalImg.alt = newImage.alt || 'Screenshot';

                const tempImg = new Image();
                tempImg.onload = function() {
                    scaleModalImage(modalImg, tempImg.width, tempImg.height);
                };
                tempImg.src = url;
            }

            if (currentNumEl) {
                currentNumEl.textContent = currentImageIndex + 1;
            }
        }
    }

    modal.addEventListener('hidden.bs.modal', function() {
        window.cardGalleryImages = null;
        window.cardGalleryCurrentIndex = null;
    });

    console.log('✅ Modal handlers configured');
}

// Реакция на поворот экрана
function refreshModalImage() {
    const modal = document.getElementById('screenshotModal');
    if (!modal || !modal.classList.contains('show')) return;

    const modalImg = document.getElementById('modalImage');
    if (modalImg && modalImg.src) {
        const tempImg = new Image();
        tempImg.onload = function() {
            scaleModalImage(modalImg, tempImg.width, tempImg.height);
        };
        tempImg.src = modalImg.src;
    }
}

// Основная функция инициализации
window.initInlineGallery = function() {
    console.log('🚀 initInlineGallery() called');

    galleryImages = Array.from(document.querySelectorAll(
        '.game-carousel-cover, .game-carousel-screenshot, ' +
        '.screenshot-image, .screenshot-image-fixed, img[data-image], ' +
        '.gallery-img'
    ));

    console.log(`📸 Found ${galleryImages.length} gallery images`);

    setupModalHandlers();
    initDetailPageGallery();

    galleryInitialized = true;
};

window.addEventListener('resize', function() {
    refreshModalImage();
});

window.addEventListener('orientationchange', function() {
    setTimeout(refreshModalImage, 50);
});

document.addEventListener('DOMContentLoaded', function() {
    console.log('🔄 DOM loaded, initializing gallery');
    if (window.initInlineGallery) {
        window.initInlineGallery();
    }
});

window.addEventListener('load', function() {
    console.log('🔄 Window loaded, reinitializing gallery');
    if (window.initInlineGallery) {
        window.initInlineGallery();
    }
});

console.log('✅ Gallery script ready');