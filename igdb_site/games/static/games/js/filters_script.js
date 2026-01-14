// games/static/games/js/filters_script.js
import FilterSort from './modules/filters-sort.js';
import FilterSearch from './modules/filters-search.js';
import FilterUI from './modules/filters-ui.js';
import FilterHandlers from './modules/filters-handlers.js';

// Глобальный объект для доступа из других скриптов
window.FilterManager = {
    sort: null,
    search: null,
    ui: null,
    handlers: null,
    initialized: false
};

// Функция для безопасного восстановления прокрутки
function safeRestoreScrollPosition() {
    if (window.FilterManager &&
        window.FilterManager.handlers &&
        typeof window.FilterManager.handlers.restoreScrollPosition === 'function') {

        console.log('Attempting to restore scroll position...');
        window.FilterManager.handlers.restoreScrollPosition();
        return true;
    } else {
        console.warn('Cannot restore scroll: handlers not ready');
        return false;
    }
}

// Функция для принудительного восстановления прокрутки с защитой
function forceScrollRestoration() {
    // Первая попытка
    let restored = safeRestoreScrollPosition();

    if (!restored) {
        // Вторая попытка через 100мс
        setTimeout(() => {
            restored = safeRestoreScrollPosition();

            if (!restored) {
                // Третья попытка через 300мс
                setTimeout(() => {
                    safeRestoreScrollPosition();
                }, 300);
            }
        }, 100);
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

            // Сохраняем в глобальный объект
            window.FilterManager.sort = sort;
            window.FilterManager.search = search;
            window.FilterManager.ui = ui;
            window.FilterManager.handlers = handlers;

            console.log('Modules loaded into FilterManager');

            // 1. Инициализация UI (секции, кнопки, поиск)
            ui.initializeAllUI();

            // 2. Настройка обработчиков
            handlers.initializeAllHandlers();

            // 3. Настройка поиска
            search.setupSearchFilters();

            // 4. Первоначальная сортировка с задержкой
            setTimeout(() => {
                if (sort && typeof sort.sortFilterLists === 'function') {
                    sort.sortFilterLists();
                }

                // Восстанавливаем прокрутку после инициализации
                setTimeout(() => {
                    forceScrollRestoration();
                }, 200);

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
export { FilterSort, FilterSearch, FilterUI, FilterHandlers };