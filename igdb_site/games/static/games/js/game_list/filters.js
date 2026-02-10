// games/static/games/js/game_list/filters.js

// Главный файл-загрузчик для модулей фильтров
// Реэкспортирует все модули и предоставляет точку входа

// Импортируем все модули
import FilterGlobal from './filters_js/filters-global.js';
import FilterHandlers from './filters_js/filters-handlers.js';
import FilterSearch from './filters_js/filters-search.js';
import FilterSort from './filters_js/filters-sort.js';
import FilterSticky from './filters_js/filters-sticky.js';
import FilterUI from './filters_js/filters-ui.js';

// Главный менеджер фильтров
const FilterManager = {
    global: FilterGlobal,
    handlers: FilterHandlers,
    search: FilterSearch,
    sort: FilterSort,
    sticky: FilterSticky,
    ui: FilterUI,

    // Инициализация всех модулей
    init() {
        console.log('🚀 Initializing FilterManager with all modules...');

        // Порядок инициализации важен
        try {
            // 1. Глобальные функции и обработчики формы
            if (FilterGlobal && typeof FilterGlobal.initializeAll === 'function') {
                FilterGlobal.initializeAll();
                console.log('✓ FilterGlobal initialized');
            }

            // 2. UI компоненты
            if (FilterUI && typeof FilterUI.initializeAllUI === 'function') {
                FilterUI.initializeAllUI();
                console.log('✓ FilterUI initialized');
            }

            // 3. Обработчики событий
            if (FilterHandlers && typeof FilterHandlers.initializeAllHandlers === 'function') {
                FilterHandlers.initializeAllHandlers();
                console.log('✓ FilterHandlers initialized');
            }

            // 4. Поиск в фильтрах
            if (FilterSearch && typeof FilterSearch.setupSearchFilters === 'function') {
                FilterSearch.setupSearchFilters();
                console.log('✓ FilterSearch initialized');
            }

            // 5. Сортировка (после загрузки всех данных)
            setTimeout(() => {
                if (FilterSort && typeof FilterSort.sortFilterLists === 'function') {
                    FilterSort.sortFilterLists();
                    console.log('✓ FilterSort initialized');
                }
            }, 500);

            // 6. Sticky кнопки (после полной загрузки)
            setTimeout(() => {
                if (FilterSticky && typeof FilterSticky.init === 'function') {
                    FilterSticky.init();
                    console.log('✓ FilterSticky initialized');
                }
            }, 1000);

            console.log('✅ All filter modules initialized successfully');

        } catch (error) {
            console.error('❌ Error initializing filter modules:', error);
        }
    }
};

// Делаем доступным глобально
window.FilterManager = FilterManager;
window.FilterGlobal = FilterGlobal;
window.FilterHandlers = FilterHandlers;
window.FilterSearch = FilterSearch;
window.FilterSort = FilterSort;
window.FilterSticky = FilterSticky;
window.FilterUI = FilterUI;

// Глобальная функция для обратной совместимости
window.initializeFilters = function() {
    console.log('🔄 Calling initializeFilters (legacy)...');
    FilterManager.init();
};

// Автоматическая инициализация при загрузке
document.addEventListener('DOMContentLoaded', function() {
    // Даем время на загрузку всех зависимостей
    setTimeout(() => {
        if (document.querySelector('#main-search-form') ||
            document.querySelector('.games-container')) {
            console.log('⚡ Auto-initializing filters...');
            FilterManager.init();
        }
    }, 300);
});

// Экспорт для ES6 модулей
export default FilterManager;
export {
    FilterGlobal,
    FilterHandlers,
    FilterSearch,
    FilterSort,
    FilterSticky,
    FilterUI
};