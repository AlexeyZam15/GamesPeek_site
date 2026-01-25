// games/static/games/js/filters_script.js
import FilterSort from './game_list/filters-sort.js';
import FilterSearch from './game_list/filters-search.js';
import FilterUI from './game_list/filters-ui.js';
import FilterHandlers from './game_list/filters-handlers.js';
import FilterSticky from './game_list/filters-sticky.js';
import KeywordsPagination from './game_list/keywords-pagination.js';
import GamePagination from './game_list/game-pagination.js';

// Создаем глобальный объект FilterManager для доступа из других скриптов
window.FilterManager = {
    sort: FilterSort,
    search: FilterSearch,
    ui: FilterUI,
    handlers: FilterHandlers,
    sticky: FilterSticky,
    keywordsPagination: KeywordsPagination,
    gamePagination: GamePagination
};

// Сделаем доступным глобально для других модулей
window.KeywordsPagination = KeywordsPagination;
window.GamePagination = GamePagination;

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

            // Инициализация пагинации ключевых слов с задержкой
            if (KeywordsPagination && typeof KeywordsPagination.init === 'function') {
                console.log('Initializing keywords pagination...');
                setTimeout(() => {
                    try {
                        KeywordsPagination.init();

                        // Проверяем высоту контейнера
                        const keywordList = document.querySelector('.keyword-list');
                        if (keywordList) {
                            // Рассчитываем нужную высоту для 30 элементов
                            const items = document.querySelectorAll('.keyword-item');
                            const firstItem = items[0];
                            if (firstItem) {
                                const itemHeight = firstItem.offsetHeight || 30;
                                const neededHeight = Math.min(itemHeight * 30 + 50, 400);
                                keywordList.style.height = `${neededHeight}px`;
                                keywordList.style.minHeight = `${neededHeight}px`;
                                console.log(`Set keyword list height to ${neededHeight}px for 30 items`);
                            }
                        }
                    } catch (error) {
                        console.error('Error initializing keywords pagination:', error);
                    }
                }, 300);
            }

            // Инициализация пагинации игр с задержкой
            if (GamePagination && typeof GamePagination.init === 'function') {
                console.log('Initializing games pagination...');
                setTimeout(() => {
                    try {
                        // Проверяем, есть ли контейнер с играми
                        const gamesContainer = document.querySelector('.games-container');
                        if (gamesContainer) {
                            // Добавляем CSS класс ко всем игровым карточкам для селектора
                            const gameCards = document.querySelectorAll('.col-xl-3.col-lg-4.col-md-6.mb-4');
                            gameCards.forEach((card, index) => {
                                card.classList.add('game-card-container');
                            });

                            GamePagination.init();
                        }
                    } catch (error) {
                        console.error('Error initializing games pagination:', error);
                    }
                }, 500);
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
    FilterSticky,
    KeywordsPagination,
    GamePagination
};