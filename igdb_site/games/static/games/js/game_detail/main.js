// games/static/games/js/game_detail/main.js
console.log('🎮 Game Detail Main Script Initializing');

// Инициализация всех компонентов при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    console.log('🏠 DOM Content Loaded - initializing game detail components...');

    // Инициализация галереи
    setTimeout(() => {
        console.log('⏰ GALLERY: Timeout finished - initializing gallery');

        if (window.initInlineGallery) {
            window.initInlineGallery();
        } else {
            console.warn('⚠️ initInlineGallery function not found');
        }

        // Настраиваем кнопки навигации модального окна
        const prevBtn = document.getElementById('prevBtn');
        const nextBtn = document.getElementById('nextBtn');

        if (prevBtn) {
            prevBtn.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                if (window.navigateGallery) {
                    window.navigateGallery('prev');
                }
            });
        }

        if (nextBtn) {
            nextBtn.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                if (window.navigateGallery) {
                    window.navigateGallery('next');
                }
            });
        }

        // Клавиатурная навигация
        document.addEventListener('keydown', function(e) {
            const modal = document.getElementById('screenshotModal');
            if (modal && modal.classList.contains('show')) {
                if (e.key === 'ArrowLeft' && window.navigateGallery) {
                    e.preventDefault();
                    window.navigateGallery('prev');
                } else if (e.key === 'ArrowRight' && window.navigateGallery) {
                    e.preventDefault();
                    window.navigateGallery('next');
                } else if (e.key === 'Escape') {
                    const modalInstance = bootstrap.Modal.getInstance(modal);
                    if (modalInstance) {
                        modalInstance.hide();
                    }
                }
            }
        });

    }, 1500);

    // Также инициализируем при полной загрузке страницы
    window.addEventListener('load', function() {
        console.log('🔄 GALLERY: Window loaded - reinitializing gallery');
        setTimeout(() => {
            if (window.initInlineGallery) {
                window.initInlineGallery();
            }
        }, 500);
    });

    // Инициализация основного скрипта
    if (window.initializeGameDetail) {
        window.initializeGameDetail();
    } else {
        console.warn('⚠️ initializeGameDetail function not found');
    }
});

console.log('✅ Game Detail Main Script ready');