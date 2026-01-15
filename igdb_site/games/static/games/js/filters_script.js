// games/static/games/js/filters_script.js
import FilterSort from './modules/filters-sort.js';
import FilterSearch from './modules/filters-search.js';
import FilterUI from './modules/filters-ui.js';
import FilterHandlers from './modules/filters-handlers.js';
import GameListScript from './modules/game_list_script.js';
import FilterSticky from './modules/filters-sticky.js'; // НОВЫЙ ИМПОРТ

// Глобальный объект для доступа из других скриптов
window.FilterManager = {
    sort: null,
    search: null,
    ui: null,
    handlers: null,
    gameList: null,
    sticky: null, // НОВОЕ ПОЛЕ
    initialized: false
};

// Функция для безопасного восстановления прокрутки
function safeRestoreScrollPosition() {
    if (window.FilterManager &&
        window.FilterManager.sticky &&
        typeof window.FilterManager.sticky.restoreScrollPosition === 'function') {

        console.log('Attempting to restore scroll position...');
        window.FilterManager.sticky.restoreScrollPosition();
        return true;
    } else {
        console.warn('Cannot restore scroll: sticky not ready');
        return false;
    }
}

document.addEventListener('DOMContentLoaded', function() {
    console.log('Filters script initialized - Modular ES6 version');

    // Инициализация всех модулей
    function initializeAll() {
        console.log('Initializing all modules...');

        try {
            // Инициализируем модули
            const sort = FilterSort;
            const search = FilterSearch;
            const ui = FilterUI;
            const handlers = FilterHandlers;
            const gameList = GameListScript;
            const sticky = FilterSticky; // НОВЫЙ МОДУЛЬ

            // Сохраняем в глобальный объект
            window.FilterManager.sort = sort;
            window.FilterManager.search = search;
            window.FilterManager.ui = ui;
            window.FilterManager.handlers = handlers;
            window.FilterManager.gameList = gameList;
            window.FilterManager.sticky = sticky; // СОХРАНЯЕМ

            console.log('All modules loaded into FilterManager');

            // 1. Инициализация UI (секции, кнопки, поиск)
            if (ui && typeof ui.initializeAllUI === 'function') {
                ui.initializeAllUI();
            } else {
                console.warn('FilterUI module not properly loaded');
            }

            // 2. Настройка обработчиков
            if (handlers && typeof handlers.initializeAllHandlers === 'function') {
                handlers.initializeAllHandlers();
            } else {
                console.warn('FilterHandlers module not properly loaded');
            }

            // 3. Настройка поиска
            if (search && typeof search.setupSearchFilters === 'function') {
                search.setupSearchFilters();
            } else {
                console.warn('FilterSearch module not properly loaded');
            }

            // 4. Инициализация sticky button с задержкой
            setTimeout(() => {
                if (sticky && typeof sticky.init === 'function') {
                    sticky.init();
                    console.log('Sticky button initialized');
                } else {
                    console.warn('FilterSticky module not properly loaded');
                }
            }, 500);

            // 5. Первоначальная сортировка с задержкой
            setTimeout(() => {
                if (sort && typeof sort.sortFilterLists === 'function') {
                    sort.sortFilterLists();
                    console.log('Initial sorting completed');
                } else {
                    console.warn('FilterSort module not properly loaded');
                }

                window.FilterManager.initialized = true;
                console.log('All modules initialized successfully');

                // Отправляем событие, что фильтры готовы
                document.dispatchEvent(new CustomEvent('filtersReady'));
            }, 200);

        } catch (error) {
            console.error('Error initializing filter modules:', error);
        }
    }

    // Запускаем инициализацию с задержкой
    setTimeout(initializeAll, 100);
});

// Также пробуем восстановить прокрутку при полной загрузке страницы
window.addEventListener('load', () => {
    setTimeout(() => {
        safeRestoreScrollPosition();
    }, 300);
});

// Функция для проверки готовности фильтров
function waitForFiltersReady(callback) {
    if (window.FilterManager.initialized) {
        callback();
    } else {
        document.addEventListener('filtersReady', callback);
    }
}

// Экспорт для тестирования
export {
    FilterSort,
    FilterSearch,
    FilterUI,
    FilterHandlers,
    GameListScript,
    FilterSticky // НОВЫЙ ЭКСПОРТ
};