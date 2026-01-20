// games/static/games/js/filters_script.js
import FilterSort from './modules/filters-sort.js';
import FilterSearch from './modules/filters-search.js';
import FilterUI from './modules/filters-ui.js';
import FilterHandlers from './modules/filters-handlers.js';
import FilterSticky from './modules/filters-sticky.js';

// Создаем глобальный объект FilterManager для доступа из других скриптов
window.FilterManager = {
    sort: FilterSort,
    search: FilterSearch,
    ui: FilterUI,
    handlers: FilterHandlers,
    sticky: FilterSticky
};

// Флаг чтобы скрипт не запускался несколько раз
let initialized = false;

document.addEventListener('DOMContentLoaded', function() {
    if (initialized) return;
    initialized = true;

    // Инициализация с задержкой для полной загрузки DOM
    setTimeout(initializeAll, 500);

    function initializeAll() {
        try {
            console.log('=== FILTERS SCRIPT INITIALIZATION START ===');

            // Инициализация основных модулей
            if (FilterUI && typeof FilterUI.initializeAllUI === 'function') {
                console.log('Initializing UI...');
                FilterUI.initializeAllUI();
            }

            if (FilterHandlers && typeof FilterHandlers.initializeAllHandlers === 'function') {
                console.log('Initializing handlers...');
                FilterHandlers.initializeAllHandlers();
            }

            if (FilterSearch && typeof FilterSearch.setupSearchFilters === 'function') {
                console.log('Setting up search filters...');
                FilterSearch.setupSearchFilters();
            }

            // Sticky кнопки инициализируем с задержкой
            setTimeout(() => {
                if (FilterSticky && typeof FilterSticky.init === 'function') {
                    console.log('Initializing sticky buttons...');
                    FilterSticky.init();
                }
            }, 800);

            // Сортировка фильтров - вызываем после всех инициализаций
            setTimeout(() => {
                if (FilterSort && typeof FilterSort.sortFilterLists === 'function') {
                    console.log('Initial sort of filter lists...');
                    FilterSort.sortFilterLists();
                }

                // Восстановление прокрутки
                restoreScrollPosition();
            }, 1000);

            console.log('=== FILTERS SCRIPT INITIALIZATION COMPLETE ===');

        } catch (error) {
            console.error('Error in filters script initialization:', error);
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
                        console.log('Scroll position restored to:', y);
                    }, 300);
                }
            }
        } catch (e) {
            console.warn('Could not restore scroll position:', e);
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