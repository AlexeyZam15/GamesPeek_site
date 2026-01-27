// games/static/games/js/game_list.js
import FilterSort from './game_list/filters-sort.js';
import FilterSearch from './game_list/filters-search.js';
import FilterUI from './game_list/filters-ui.js';
import FilterHandlers from './game_list/filters-handlers.js';
import FilterSticky from './game_list/filters-sticky.js';
import KeywordsPagination from './game_list/keywords-pagination.js';
import GamePagination from './game_list/game-pagination.js';

window.FilterManager = {
    sort: FilterSort,
    search: FilterSearch,
    ui: FilterUI,
    handlers: FilterHandlers,
    sticky: FilterSticky,
    keywordsPagination: KeywordsPagination,
    gamePagination: GamePagination
};

window.KeywordsPagination = KeywordsPagination;
window.GamePagination = GamePagination;

let initialized = false;

document.addEventListener('DOMContentLoaded', function() {
    if (initialized) return;
    initialized = true;

    setTimeout(initializeAll, 800);

    function initializeAll() {
        try {
            console.log('=== FILTERS SCRIPT INITIALIZATION START ===');

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

            if (KeywordsPagination && typeof KeywordsPagination.init === 'function') {
                console.log('Initializing keywords pagination...');
                setTimeout(() => {
                    try {
                        KeywordsPagination.init();
                    } catch (error) {
                        console.error('Error initializing keywords pagination:', error);
                    }
                }, 300);
            }

            if (GamePagination && typeof GamePagination.init === 'function') {
                console.log('Initializing LAZY games pagination...');

                setTimeout(() => {
                    try {
                        const gamesContainer = document.querySelector('.games-container');
                        if (gamesContainer) {
                            // Убедимся что все карточки имеют правильный класс
                            const existingCards = document.querySelectorAll('.col-xl-3.col-lg-4.col-md-6.mb-4');
                            existingCards.forEach((card, index) => {
                                if (!card.classList.contains('game-card-container')) {
                                    card.classList.add('game-card-container');
                                }
                            });

                            // ВАЖНО: Сохраняем текущую позицию прокрутки
                            const scrollBefore = window.scrollY;

                            GamePagination.init();
                            console.log('Lazy games pagination initialized successfully');

                            // Восстанавливаем позицию прокрутки через 100мс
                            setTimeout(() => {
                                restoreScrollPosition();
                            }, 100);

                            // Сохраняем первую страницу в кэш
                            saveCurrentPageToCache();
                        } else {
                            console.warn('Games container not found');
                        }
                    } catch (error) {
                        console.error('Error initializing lazy games pagination:', error);
                    }
                }, 500); // Уменьшили задержку с 1000 до 500
            }

            setTimeout(() => {
                if (FilterSticky && typeof FilterSticky.init === 'function') {
                    console.log('Initializing sticky buttons...');
                    FilterSticky.init();
                }
            }, 800); // Уменьшили задержку

            setTimeout(() => {
                if (FilterSort && typeof FilterSort.sortFilterLists === 'function') {
                    console.log('Initial sort of filter lists...');
                    FilterSort.sortFilterLists();
                }
            }, 1200); // Уменьшили задержку

            console.log('=== FILTERS SCRIPT INITIALIZATION COMPLETE ===');

        } catch (error) {
            console.error('Error in filters script initialization:', error);
        }
    }

    function saveCurrentPageToCache() {
        try {
            const gamesContainer = document.querySelector('.games-container');
            if (!gamesContainer) return;

            const gameElements = gamesContainer.querySelectorAll('.game-card-container');
            const games = Array.from(gameElements).map(el => el.outerHTML);

            sessionStorage.setItem('page_1_games', JSON.stringify(games));
            console.log('Saved current page (page 1) to cache');
        } catch (error) {
            console.error('Error saving current page to cache:', error);
        }
    }

    function showBackgroundLoadingMessage() {
        const gamesContainer = document.querySelector('.games-container');
        if (!gamesContainer) return;

        const message = document.createElement('div');
        message.className = 'page-loading-message';
        message.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Остальные страницы загружаются в фоне...';
        message.style.cssText = `
            font-size: 0.85rem;
            color: rgba(255, 107, 53, 0.8);
            margin-top: 1rem;
            text-align: center;
            font-style: italic;
            background: rgba(255, 107, 53, 0.05);
            padding: 0.5rem 1rem;
            border-radius: 10px;
            border: 1px solid rgba(255, 107, 53, 0.2);
            animation: fadeIn 0.5s ease;
        `;

        gamesContainer.parentNode.insertBefore(message, gamesContainer.nextSibling);

        setTimeout(() => {
            if (message.parentNode) {
                message.style.opacity = '0';
                message.style.transition = 'opacity 0.5s';
                setTimeout(() => {
                    if (message.parentNode) {
                        message.parentNode.removeChild(message);
                    }
                }, 500);
            }
        }, 5000);
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

// ОБРАБОТЧИК ДЛЯ ИЗМЕНЕНИЯ ФИЛЬТРОВ
document.addEventListener('filterApplied', () => {
    console.log('Filter applied event received');
    // Сбрасываем пагинацию на первую страницу
    if (window.GamePagination) {
        console.log('Resetting pagination to page 1...');
        window.GamePagination.resetToFirstPage();
    }
});

// Обработчик для кнопок браузера "назад/вперед"
window.addEventListener('popstate', (event) => {
    console.log('Popstate event detected, state:', event.state);

    if (event.state && event.state.page && window.GamePagination) {
        // Восстанавливаем страницу из истории
        const pageNumber = event.state.page;
        console.log(`Restoring page ${pageNumber} from browser history`);

        if (window.GamePagination.state.loadedPages.has(pageNumber)) {
            window.GamePagination.showPage(pageNumber, false);
        } else {
            window.GamePagination.forceLoadPage(pageNumber);
        }
    }
});

// Обработчик для обновления пагинации при изменении DOM
const observer = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => {
        if (mutation.type === 'childList' && mutation.addedNodes.length > 0) {
            // Проверяем, добавились ли элементы пагинации
            const addedPagination = Array.from(mutation.addedNodes).some(node =>
                node.nodeType === Node.ELEMENT_NODE &&
                (node.classList?.contains('games-pagination') ||
                 node.querySelector?.('.games-pagination'))
            );

            if (addedPagination && window.GamePagination) {
                console.log('Pagination elements added to DOM, updating...');
                setTimeout(() => {
                    window.GamePagination.forceUpdate();
                }, 100);
            }
        }
    });
});

// Начинаем наблюдение за изменениями в body
observer.observe(document.body, {
    childList: true,
    subtree: true
});

export {
    FilterSort,
    FilterSearch,
    FilterUI,
    FilterHandlers,
    FilterSticky,
    KeywordsPagination,
    GamePagination
};