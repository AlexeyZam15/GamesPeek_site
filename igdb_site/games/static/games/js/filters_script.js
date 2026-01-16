// games/static/games/js/filters_script.js
import FilterSort from './modules/filters-sort.js';
import FilterSearch from './modules/filters-search.js';
import FilterUI from './modules/filters-ui.js';
import FilterHandlers from './modules/filters-handlers.js';
import FilterSticky from './modules/filters-sticky.js';

// Флаг чтобы скрипт не запускался несколько раз
let initialized = false;

document.addEventListener('DOMContentLoaded', function() {
    if (initialized) return;
    initialized = true;

    // Инициализация с задержкой для полной загрузки DOM
    setTimeout(initializeAll, 500);

    function initializeAll() {
        try {
            // Инициализация основных модулей
            if (FilterUI && typeof FilterUI.initializeAllUI === 'function') {
                FilterUI.initializeAllUI();
            }

            if (FilterHandlers && typeof FilterHandlers.initializeAllHandlers === 'function') {
                FilterHandlers.initializeAllHandlers();
            }

            if (FilterSearch && typeof FilterSearch.setupSearchFilters === 'function') {
                FilterSearch.setupSearchFilters();
            }

            // Sticky кнопки инициализируем с задержкой
            setTimeout(() => {
                if (FilterSticky && typeof FilterSticky.init === 'function') {
                    FilterSticky.init();
                }
            }, 800);

            // Сортировка фильтров
            setTimeout(() => {
                if (FilterSort && typeof FilterSort.sortFilterLists === 'function') {
                    FilterSort.sortFilterLists();
                }

                // Восстановление прокрутки
                restoreScrollPosition();
            }, 300);

        } catch (error) {
            // Тихий фейл
        }
    }

    function restoreScrollPosition() {
        try {
            const saved = sessionStorage.getItem('filterScrollY');
            if (saved) {
                const y = parseInt(saved);
                if (!isNaN(y) && y > 0) {
                    setTimeout(() => {
                        window.scrollTo({ top: y, behavior: 'smooth' });
                        sessionStorage.removeItem('filterScrollY');
                    }, 300);
                }
            }
        } catch (e) {
            // Игнорируем ошибки
        }
    }
});

// Экспорт для использования в других модулях
export {
    FilterSort,
    FilterSearch,
    FilterUI,
    FilterHandlers,
    FilterSticky
};