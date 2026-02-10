// games/static/games/js/game_list/scroll-position-manager.js
/**
 * Менеджер позиции прокрутки для серверной пагинации
 * Сохраняет и восстанавливает позицию прокрутки при переходах между страницами
 */

const ScrollPositionManager = {
    // Ключ для localStorage
    storageKey: 'games_page_scroll_position',
    // Время жизни сохраненной позиции (5 минут)
    maxAge: 5 * 60 * 1000,
    // Минимальная высота прокрутки для сохранения (чтобы не сохранять позицию у самого верха)
    minScrollHeight: 100,

    init() {
        console.log('Initializing scroll position manager...');

        // Восстанавливаем позицию при загрузке страницы
        this.restoreScrollPosition();

        // Сохраняем позицию при уходе со страницы
        this.setupBeforeUnload();

        // Сохраняем позицию при клике на пагинацию
        this.setupPaginationClicks();

        console.log('Scroll position manager initialized');
    },

    /**
     * Сохраняет текущую позицию прокрутки
     */
    saveScrollPosition() {
        const scrollY = window.scrollY;

        // Не сохраняем если прокрутка слишком маленькая
        if (scrollY < this.minScrollHeight) {
            this.clearScrollPosition();
            return;
        }

        const pageData = {
            scrollY: scrollY,
            timestamp: Date.now(),
            pageUrl: window.location.pathname + window.location.search,
            pageNumber: this.getCurrentPageNumber()
        };

        try {
            localStorage.setItem(this.storageKey, JSON.stringify(pageData));
            console.log(`Saved scroll position: ${scrollY}px for page ${pageData.pageNumber}`);
        } catch (e) {
            console.warn('Could not save scroll position to localStorage:', e);
        }
    },

    /**
     * Восстанавливает сохраненную позицию прокрутки
     */
    restoreScrollPosition() {
        try {
            const savedData = localStorage.getItem(this.storageKey);
            if (!savedData) return;

            const data = JSON.parse(savedData);

            // Проверяем актуальность данных
            const isExpired = Date.now() - data.timestamp > this.maxAge;
            const isSamePage = data.pageUrl === (window.location.pathname + window.location.search);

            if (isExpired || !isSamePage) {
                this.clearScrollPosition();
                return;
            }

            // Не восстанавливаем если мы уже ниже сохраненной позиции
            // (например, если пользователь уже прокрутил страницу)
            if (window.scrollY > data.scrollY) {
                this.clearScrollPosition();
                return;
            }

            // Ждем пока загрузятся все изображения и контент
            window.addEventListener('load', () => {
                setTimeout(() => {
                    // Плавная прокрутка к сохраненной позиции
                    window.scrollTo({
                        top: data.scrollY,
                        behavior: 'smooth'
                    });

                    console.log(`Restored scroll position to: ${data.scrollY}px`);

                    // Очищаем после восстановления
                    this.clearScrollPosition();
                }, 100);
            });

        } catch (e) {
            console.warn('Could not restore scroll position:', e);
            this.clearScrollPosition();
        }
    },

    /**
     * Очищает сохраненную позицию прокрутки
     */
    clearScrollPosition() {
        try {
            localStorage.removeItem(this.storageKey);
        } catch (e) {
            console.warn('Could not clear scroll position:', e);
        }
    },

    /**
     * Получает номер текущей страницы из URL или DOM
     */
    getCurrentPageNumber() {
        // Пробуем получить из URL
        const urlParams = new URLSearchParams(window.location.search);
        const pageFromUrl = urlParams.get('page');
        if (pageFromUrl) return parseInt(pageFromUrl);

        // Пробуем получить из DOM
        const currentPageElement = document.querySelector('#games-current-top, #games-current');
        if (currentPageElement) {
            const pageText = currentPageElement.textContent.trim();
            const pageMatch = pageText.match(/\d+/);
            if (pageMatch) return parseInt(pageMatch[0]);
        }

        return 1;
    },

    /**
     * Настраивает сохранение позиции перед уходом со страницы
     */
    setupBeforeUnload() {
        // Сохраняем при уходе со страницы
        window.addEventListener('beforeunload', () => {
            this.saveScrollPosition();
        });

        // Также сохраняем периодически при прокрутке
        let scrollSaveTimeout;
        window.addEventListener('scroll', () => {
            clearTimeout(scrollSaveTimeout);
            scrollSaveTimeout = setTimeout(() => {
                this.saveScrollPosition();
            }, 500); // Дебаунс 500мс
        });
    },

    /**
     * Настраивает обработку кликов по пагинации
     */
    setupPaginationClicks() {
        document.querySelectorAll('.games-pagination a.page-link').forEach(link => {
            link.addEventListener('click', (e) => {
                // Проверяем, что это ссылка на другую страницу (не текущая и не disabled)
                const href = link.getAttribute('href');
                if (!href || link.classList.contains('disabled')) return;

                // Проверяем, ведет ли ссылка на другую страницу
                const currentUrl = new URL(window.location.href);
                const targetUrl = new URL(href, window.location.origin);

                if (currentUrl.pathname === targetUrl.pathname &&
                    currentUrl.searchParams.get('page') !== targetUrl.searchParams.get('page')) {
                    // Сохраняем позицию прокрутки
                    this.saveScrollPosition();

                    // Добавляем небольшую задержку для сохранения
                    setTimeout(() => {
                        // Переход произойдет автоматически по ссылке
                    }, 100);
                }
            });
        });
    },

    /**
     * Сохраняет позицию определенного элемента на странице
     * @param {HTMLElement} element - Элемент, позицию которого нужно сохранить
     * @param {string} elementId - Уникальный ID элемента
     */
    saveElementPosition(element, elementId) {
        if (!element || !elementId) return;

        const rect = element.getBoundingClientRect();
        const scrollY = window.scrollY;
        const elementTop = rect.top + scrollY;

        const elementData = {
            elementId: elementId,
            elementTop: elementTop,
            scrollY: scrollY,
            timestamp: Date.now(),
            pageUrl: window.location.pathname + window.location.search
        };

        try {
            localStorage.setItem(`${this.storageKey}_element_${elementId}`, JSON.stringify(elementData));
            console.log(`Saved element position: ${elementId} at ${elementTop}px`);
        } catch (e) {
            console.warn('Could not save element position:', e);
        }
    },

    /**
     * Восстанавливает позицию к определенному элементу
     * @param {string} elementId - ID элемента для восстановления позиции
     */
    restoreElementPosition(elementId) {
        try {
            const savedData = localStorage.getItem(`${this.storageKey}_element_${elementId}`);
            if (!savedData) return false;

            const data = JSON.parse(savedData);

            // Проверяем актуальность
            const isExpired = Date.now() - data.timestamp > this.maxAge;
            const isSamePage = data.pageUrl === (window.location.pathname + window.location.search);

            if (isExpired || !isSamePage) {
                localStorage.removeItem(`${this.storageKey}_element_${elementId}`);
                return false;
            }

            // Ищем элемент
            const element = document.getElementById(elementId);
            if (!element) {
                localStorage.removeItem(`${this.storageKey}_element_${elementId}`);
                return false;
            }

            // Плавная прокрутка к элементу
            window.addEventListener('load', () => {
                setTimeout(() => {
                    const currentRect = element.getBoundingClientRect();
                    const targetScroll = currentRect.top + window.scrollY - 100; // Минус 100px для отступа сверху

                    window.scrollTo({
                        top: targetScroll,
                        behavior: 'smooth'
                    });

                    console.log(`Restored scroll to element: ${elementId}`);
                    localStorage.removeItem(`${this.storageKey}_element_${elementId}`);
                }, 150);
            });

            return true;

        } catch (e) {
            console.warn('Could not restore element position:', e);
            return false;
        }
    }
};

// Инициализация при загрузке DOM
document.addEventListener('DOMContentLoaded', function() {
    ScrollPositionManager.init();
});

// Экспорт для использования в других модулях
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ScrollPositionManager;
} else {
    window.ScrollPositionManager = ScrollPositionManager;
}