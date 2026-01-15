// games/static/games/js/modules/filters-sticky.js

const FilterSticky = {
    init() {
        console.log('Initializing Sticky Button...');

        this.container = document.getElementById('sticky-apply-btn-container');
        this.button = document.querySelector('.apply-filters-sticky');

        if (!this.container || !this.button) {
            console.error('Sticky button elements not found!');
            return;
        }

        // Принудительно устанавливаем стили
        this.container.style.position = 'fixed';
        this.container.style.bottom = '30px';
        this.container.style.left = '50%';
        this.container.style.transform = 'translateX(-50%)';
        this.container.style.zIndex = '999999';
        this.container.style.display = 'block';
        this.container.style.opacity = '1';
        this.container.style.visibility = 'visible';

        this.setupClickHandler();

        console.log('Sticky Button initialized');
    },

    setupClickHandler() {
        this.button.addEventListener('click', (e) => {
            e.preventDefault();
            this.handleApplyClick();
        });
    },

    handleApplyClick() {
        const form = document.getElementById('main-search-form');
        if (!form) {
            console.error('Main search form not found');
            return;
        }

        // Сохраняем позицию прокрутки
        try {
            const scrollY = window.scrollY || document.documentElement.scrollTop;
            if (scrollY > 100) {
                sessionStorage.setItem('filterScrollY', scrollY.toString());
            }
        } catch (e) {
            console.warn('Could not save scroll position');
        }

        // Показываем состояние загрузки
        const originalText = this.button.innerHTML;
        this.button.innerHTML = '<i class="bi bi-hourglass-split me-2"></i> Applying...';
        this.button.disabled = true;

        // Добавляем скрытое поле для сохранения позиции
        let scrollField = form.querySelector('input[name="_scroll"]');
        if (!scrollField) {
            scrollField = document.createElement('input');
            scrollField.type = 'hidden';
            scrollField.name = '_scroll';
            scrollField.value = '1';
            form.appendChild(scrollField);
        }

        // Отправляем форму с небольшой задержкой
        setTimeout(() => {
            form.submit();
        }, 100);
    },

    restoreScrollPosition() {
        try {
            const saved = sessionStorage.getItem('filterScrollY');
            if (saved) {
                const y = parseInt(saved);
                if (!isNaN(y) && y > 0) {
                    console.log('Restoring scroll to:', y);

                    setTimeout(() => {
                        window.scrollTo({
                            top: y,
                            behavior: 'auto'
                        });
                        sessionStorage.removeItem('filterScrollY');
                    }, 150);
                }
            }
        } catch (e) {
            console.warn('Could not restore scroll position');
            sessionStorage.removeItem('filterScrollY');
        }
    }
};

// Авто-инициализация
document.addEventListener('DOMContentLoaded', () => {
    setTimeout(() => {
        FilterSticky.init();

        // Восстанавливаем позицию прокрутки
        if (document.readyState === 'complete') {
            setTimeout(() => {
                FilterSticky.restoreScrollPosition();
            }, 100);
        } else {
            window.addEventListener('load', () => {
                setTimeout(() => {
                    FilterSticky.restoreScrollPosition();
                }, 100);
            });
        }
    }, 500);
});

export default FilterSticky;