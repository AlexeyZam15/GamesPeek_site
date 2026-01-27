// games/static/games/js/game_list/game-pagination.js

const GamePagination = {
    // Конфигурация
    config: {
        itemsPerPage: 16,
        currentPage: 1,
        itemsSelector: '.game-card-container',
        containerSelector: '.games-container',
        // Элементы для пагинации вверху
        paginationTopSelector: '.games-pagination-top',
        prevButtonTopId: '#games-prev-top',
        nextButtonTopId: '#games-next-top',
        startElementTopId: '#games-start-top',
        endElementTopId: '#games-end-top',
        currentElementTopId: '#games-current-top',
        totalElementTopId: '#games-total-all-top',
        totalPagesElementTopId: '#games-total-pages-top',
        pageNumbersContainerTopId: '#games-page-numbers-top',
        // Элементы для пагинации внизу
        paginationBottomSelector: '.games-pagination-bottom',
        prevButtonBottomId: '#games-prev',
        nextButtonBottomId: '#games-next',
        startElementBottomId: '#games-start',
        endElementBottomId: '#games-end',
        currentElementBottomId: '#games-current',
        totalElementBottomId: '#games-total-all',
        totalPagesElementBottomId: '#games-total-pages',
        pageNumbersContainerBottomId: '#games-page-numbers',
        maxVisiblePages: 7
    },

    // Состояние
    state: {
        totalItems: 0,
        totalPages: 0,
        loadedPages: new Set(),
        isLoading: false,
        gameItems: [],
        loadingPages: new Set(),
    },

    // Инициализация
    init() {
        console.log('Initializing lazy games pagination...');

        // Полностью очищаем state
        this.state.gameItems = [];
        this.state.loadedPages.clear();

        if (!this.state.loadingPages) {
            this.state.loadingPages = new Set();
        }

        // Получаем информацию о пагинации из DOM
        this.loadPaginationInfoFromDOM();

        // Определяем текущую страницу из URL
        const urlPage = this.getPageFromURL();
        console.log(`URL requests page: ${urlPage}`);

        // Получаем контейнер
        const container = document.querySelector(this.config.containerSelector);

        // ВАЖНО: ВСЕГДА очищаем DOM при инициализации
        if (container) {
            const rowElement = container.querySelector('.row');
            if (rowElement) {
                const existingGames = rowElement.querySelectorAll('.game-card-container');
                console.log(`Found ${existingGames.length} games in DOM before initialization`);

                // Определяем, для какой страницы эти игры
                let domPage = null;
                if (existingGames.length > 0 && existingGames[0].dataset.page) {
                    domPage = parseInt(existingGames[0].dataset.page);
                    console.log(`DOM games have data-page="${domPage}"`);
                }

                // КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ:
                // Если игры в DOM принадлежат НЕ запрошенной странице - очищаем их
                if (domPage !== null && domPage !== urlPage) {
                    console.log(`CLEARING DOM: Games belong to page ${domPage}, but URL requests page ${urlPage}`);
                    rowElement.innerHTML = '';

                    // Также очищаем соответствующие игры из state
                    const startIndex = (domPage - 1) * this.config.itemsPerPage;
                    const endIndex = startIndex + this.config.itemsPerPage;
                    for (let i = startIndex; i < endIndex; i++) {
                        if (this.state.gameItems[i]) {
                            delete this.state.gameItems[i];
                        }
                    }
                }
            }
        }

        this.config.currentPage = urlPage;

        // Проверяем корректность текущей страницы
        if (this.state.totalPages > 0 && this.config.currentPage > this.state.totalPages) {
            console.log(`Adjusting page ${this.config.currentPage} to ${this.state.totalPages}`);
            this.config.currentPage = Math.max(1, this.state.totalPages);
        }

        // Создаем контейнеры для пагинации
        this.createPageNumbersContainer('top');
        this.createPageNumbersContainer('bottom');

        this.setupPagination();

        // ВСЕГДА загружаем страницу через AJAX, даже если она уже в DOM
        // Это гарантирует, что у нас будут правильные игры с правильными data-атрибутами
        console.log(`Loading page ${this.config.currentPage} via AJAX...`);
        this.showPage(this.config.currentPage, false);

        console.log(`Pagination initialized: ${this.state.totalItems} items, ${this.state.totalPages} pages, current: ${this.config.currentPage}`);

        // Сохраняем состояние
        this.saveCurrentPageToStorage(this.config.currentPage);
    },

    // Проверить, что игры в DOM принадлежат правильной странице
    verifyDOMGames(expectedPage) {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return false;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return false;

        const gameElements = rowElement.querySelectorAll('.game-card-container');
        if (gameElements.length === 0) return false;

        // Проверяем data-page атрибуты у всех игр
        for (const gameElement of gameElements) {
            const gamePage = parseInt(gameElement.dataset.page);
            if (isNaN(gamePage) || gamePage !== expectedPage) {
                console.log(`DOM verification failed: game has data-page="${gamePage}", expected ${expectedPage}`);
                return false;
            }
        }

        console.log(`DOM verification passed for page ${expectedPage}`);
        return true;
    },

    // Проверить и очистить неправильные игры из DOM
    validateAndCleanDOM(expectedPage) {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return;

        const gameElements = rowElement.querySelectorAll('.game-card-container');
        let hasWrongGames = false;
        let needsCleaning = false;

        gameElements.forEach((game, index) => {
            if (game.dataset.page) {
                const gamePage = parseInt(game.dataset.page);
                if (gamePage !== expectedPage) {
                    console.log(`Found game with wrong data-page: ${gamePage}, expected: ${expectedPage}`);
                    hasWrongGames = true;
                }
            } else {
                console.log(`Found game without data-page attribute`);
                needsCleaning = true;
            }
        });

        if (hasWrongGames || needsCleaning) {
            console.log(`Cleaning DOM because of wrong/missing data-page attributes`);
            rowElement.innerHTML = '';
            return true;
        }

        return false;
    },

    // Полная очистка кэша
    clearAllCache() {
        console.log('Clearing all pagination cache...');

        this.state.gameItems = [];
        this.state.loadedPages.clear();

        if (this.state.loadingPages) {
            this.state.loadingPages.clear();
        }

        // Очищаем sessionStorage
        try {
            const keysToRemove = [];
            for (let i = 0; i < sessionStorage.length; i++) {
                const key = sessionStorage.key(i);
                if (key.startsWith('page_') || key.startsWith('gamePagination')) {
                    keysToRemove.push(key);
                }
            }

            keysToRemove.forEach(key => {
                sessionStorage.removeItem(key);
            });

            console.log(`Removed ${keysToRemove.length} cache keys`);
        } catch (e) {
            console.warn('Error clearing sessionStorage:', e);
        }

        // Очищаем DOM контейнер
        const container = document.querySelector(this.config.containerSelector);
        if (container) {
            const rowElement = container.querySelector('.row');
            if (rowElement) {
                rowElement.innerHTML = '';
            }
        }

        console.log('All cache cleared');
    },

    // Очистить все игровые элементы из DOM
    clearAllGameElements() {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (rowElement) {
            // Сохраняем только скрытые поля и отладочную информацию
            const hiddenElements = rowElement.querySelectorAll('input[type="hidden"], #server-debug-info, #extended-debug-info');
            const hiddenElementsHTML = Array.from(hiddenElements).map(el => el.outerHTML).join('');

            rowElement.innerHTML = hiddenElementsHTML;
            console.log('Cleared all game elements from DOM, kept hidden fields');
        }
    },

    getServerPage() {
        // Пробуем получить из скрытого поля
        const serverPageElement = document.getElementById('server-current-page');
        if (serverPageElement) {
            const page = parseInt(serverPageElement.value);
            if (!isNaN(page) && page > 0) {
                return page;
            }
        }

        // Пробуем получить из data-атрибута
        const gamesContainer = document.querySelector(this.config.containerSelector);
        if (gamesContainer && gamesContainer.dataset.serverPage) {
            const page = parseInt(gamesContainer.dataset.serverPage);
            if (!isNaN(page) && page > 0) {
                return page;
            }
        }

        // Пробуем получить из window.ServerData
        if (window.ServerData && window.ServerData.pagination && window.ServerData.pagination.currentPage) {
            return window.ServerData.pagination.currentPage;
        }

        return 0;
    },

    checkIfPageHasGamesInDOM(pageNumber) {
        if (!pageNumber || pageNumber < 1) return false;

        const container = document.querySelector(this.config.containerSelector);
        if (!container) return false;

        // Проверяем, есть ли в контейнере игры
        const gameElements = container.querySelectorAll('.game-card-container');
        const gameCount = gameElements.length;

        console.log(`Found ${gameCount} game elements in DOM for page check`);

        // Если есть игры в DOM, проверяем соответствует ли их количество запрошенной странице
        if (gameCount > 0) {
            // Рассчитываем диапазон индексов для этой страницы
            const startIndex = (pageNumber - 1) * this.config.itemsPerPage;
            const endIndex = Math.min(startIndex + this.config.itemsPerPage, this.state.totalItems);

            // Если количество игр соответствует странице или это последняя страница
            if (gameCount === this.config.itemsPerPage ||
                (pageNumber === this.state.totalPages && gameCount === (this.state.totalItems % this.config.itemsPerPage || this.config.itemsPerPage))) {
                console.log(`DOM has ${gameCount} games, which matches page ${pageNumber} (range: ${startIndex + 1}-${endIndex})`);
                return true;
            }
        }

        return false;
    },

    getPageFromURL() {
        const urlParams = new URLSearchParams(window.location.search);
        const pageFromURL = urlParams.get('page');

        if (pageFromURL) {
            const parsedPage = parseInt(pageFromURL);
            if (!isNaN(parsedPage) && parsedPage >= 1) {
                // Проверяем, что страница в пределах доступных
                if (this.state.totalPages > 0 && parsedPage > this.state.totalPages) {
                    console.log(`Page ${parsedPage} exceeds total pages (${this.state.totalPages}), adjusting to ${this.state.totalPages}`);
                    return this.state.totalPages;
                }
                return parsedPage;
            }
        }

        // ВАЖНО: Если в URL нет параметра page, проверяем скрытое поле от сервера
        const serverPageElement = document.getElementById('server-current-page');
        if (serverPageElement) {
            const serverPage = parseInt(serverPageElement.value);
            if (!isNaN(serverPage) && serverPage >= 1) {
                console.log(`Using server page from hidden field: ${serverPage}`);
                return serverPage;
            }
        }

        return 1;
    },

    showLoadingIndicator(pageNumber) {
        // НЕ показываем индикатор для первой страницы
        if (pageNumber <= 1) {
            console.log('Skipping loading indicator for page 1');
            return;
        }

        // Проверяем, не показан ли уже индикатор
        const existingIndicator = document.getElementById(`loading-page-${pageNumber}`);
        if (existingIndicator) {
            console.log(`Loading indicator for page ${pageNumber} already exists`);
            return;
        }

        // Проверяем, не загружена ли уже страница
        if (this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loaded, skipping indicator`);
            return;
        }

        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        console.log(`Creating loading indicator for page ${pageNumber}`);

        // Создаем индикатор загрузки
        const loadingDiv = document.createElement('div');
        loadingDiv.className = 'page-loading-indicator';
        loadingDiv.id = `loading-page-${pageNumber}`;
        loadingDiv.innerHTML = `
            <div class="text-center py-5">
                <div class="spinner-border text-primary mb-3" style="width: 3rem; height: 3rem;" role="status">
                    <span class="visually-hidden">Loading page ${pageNumber}...</span>
                </div>
                <h5>Loading page ${pageNumber}...</h5>
                <p class="text-muted">Please wait while the games are loading</p>
            </div>
        `;
        loadingDiv.style.cssText = `
            background: rgba(255, 255, 255, 0.9);
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            z-index: 1000;
            display: flex;
            align-items: center;
            justify-content: center;
        `;

        // Находим row элемент
        let rowElement = container.querySelector('.row');
        if (!rowElement) {
            rowElement = document.createElement('div');
            rowElement.className = 'row';
            container.appendChild(rowElement);
        }

        // Очищаем контейнер и показываем индикатор
        rowElement.innerHTML = '';
        rowElement.appendChild(loadingDiv);

        // Обновляем информацию о странице
        this.updatePageNumbers();
        this.updatePageInfo();
        this.updateNavigationButtons();
    },

    // ЗАГРУЗКА ТЕКУЩЕЙ СТРАНИЦЫ ИЗ URL
    loadCurrentPageFromURL() {
        const urlPage = this.getPageFromURL();
        const serverPageElement = document.getElementById('server-current-page');
        const serverPage = serverPageElement ? parseInt(serverPageElement.value) : 1;

        // Если в URL есть параметр page, используем его
        // Иначе используем серверную страницу
        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.has('page')) {
            console.log(`Using page ${urlPage} from URL`);
            return urlPage;
        } else {
            console.log(`No page in URL, using server page ${serverPage}`);
            return serverPage;
        }
    },

    // СОХРАНЕНИЕ СТРАНИЦЫ В STORAGE
    saveCurrentPageToStorage(pageNumber) {
        try {
            const state = {
                page: pageNumber,
                timestamp: Date.now(),
                url: window.location.href
            };
            sessionStorage.setItem('gamePaginationState', JSON.stringify(state));
            console.log(`Saved page ${pageNumber} to storage`);
        } catch (e) {
            console.warn('Could not save page to storage:', e);
        }
    },

    // ВОССТАНОВЛЕНИЕ СТРАНИЦЫ ИЗ STORAGE
    restorePageFromStorage() {
        try {
            const saved = sessionStorage.getItem('gamePaginationState');
            if (saved) {
                const state = JSON.parse(saved);

                // Проверяем актуальность (не старше 5 минут)
                const age = Date.now() - state.timestamp;
                const maxAge = 5 * 60 * 1000; // 5 минут

                // Проверяем, что это тот же URL
                if (age < maxAge && state.url === window.location.href) {
                    return state.page;
                } else {
                    // Удаляем устаревшие данные
                    sessionStorage.removeItem('gamePaginationState');
                }
            }
        } catch (e) {
            console.warn('Could not restore page from storage:', e);
            sessionStorage.removeItem('gamePaginationState');
        }
        return null;
    },

    // ОЧИСТКА СТРАНИЦЫ ПРИ ИЗМЕНЕНИИ ФИЛЬТРОВ
    clearPageState() {
        try {
            sessionStorage.removeItem('gamePaginationState');
            console.log('Cleared page state from storage');
        } catch (e) {
            console.warn('Could not clear page state:', e);
        }
    },

    // Загрузка информации о пагинации из DOM
    loadPaginationInfoFromDOM() {
        // Пробуем получить из скрытых полей сервера
        const serverTotalElement = document.getElementById('server-total-count');
        const serverTotalPagesElement = document.getElementById('server-total-pages');

        if (serverTotalElement) {
            this.state.totalItems = parseInt(serverTotalElement.value) || 0;
        } else {
            const totalElement = document.querySelector(this.config.totalElementBottomId);
            if (totalElement) {
                this.state.totalItems = parseInt(totalElement.textContent) || 0;
            } else {
                const totalTopElement = document.querySelector(this.config.totalElementTopId);
                if (totalTopElement) {
                    this.state.totalItems = parseInt(totalTopElement.textContent) || 0;
                }
            }
        }

        if (serverTotalPagesElement) {
            this.state.totalPages = parseInt(serverTotalPagesElement.value) || 1;
        } else {
            const totalPagesElement = document.querySelector(this.config.totalPagesElementBottomId);
            if (totalPagesElement) {
                const totalPagesText = totalPagesElement.textContent;
                if (totalPagesText && totalPagesText.trim() !== '') {
                    this.state.totalPages = parseInt(totalPagesText) || 1;
                } else {
                    this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);
                }
            } else {
                this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);
            }
        }

        // Дополнительная проверка
        if (this.state.totalItems > this.config.itemsPerPage && this.state.totalPages <= 1) {
            this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);
            console.log(`Recalculated totalPages: ${this.state.totalPages} from ${this.state.totalItems} items`);
        }

        console.log(`Pagination info from DOM: ${this.state.totalItems} items, ${this.state.totalPages} pages`);

        this.showPagination();
    },

    // Загрузить текущие игры из DOM (первая страница)
    loadCurrentGamesFromDOM() {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) {
            console.error('Games container not found');
            return;
        }

        const rowElement = container.querySelector('.row');
        if (!rowElement) {
            console.error('Row element not found in games container');
            return;
        }

        // Получаем все текущие игры
        const gameElements = rowElement.querySelectorAll('.game-card-container');

        console.log(`Found ${gameElements.length} game elements in DOM`);

        // Определяем, для какой страницы эти игры
        const serverPage = this.getServerPage();
        const urlPage = this.getPageFromURL();
        const targetPage = serverPage > 0 ? serverPage : urlPage;

        // Рассчитываем начальный индекс для страницы
        const startIndex = (targetPage - 1) * this.config.itemsPerPage;

        // Сохраняем игры для правильной страницы
        gameElements.forEach((element, index) => {
            const gameIndex = startIndex + index;
            this.state.gameItems[gameIndex] = element;
            element.dataset.page = targetPage;
            console.log(`Added game at index ${gameIndex} for page ${targetPage}`);
        });

        console.log(`Loaded ${gameElements.length} games from current page ${targetPage}`);
    },

    // Создаем контейнер для номеров страниц
    createPageNumbersContainer(position) {
        const containerId = position === 'top' ?
            this.config.pageNumbersContainerTopId :
            this.config.pageNumbersContainerBottomId;

        const buttonId = position === 'top' ?
            this.config.prevButtonTopId :
            this.config.prevButtonBottomId;

        if (document.querySelector(containerId)) {
            return;
        }

        const paginationContainer = document.querySelector(
            position === 'top' ?
            this.config.paginationTopSelector :
            this.config.paginationBottomSelector
        );
        if (!paginationContainer) return;

        const prevButton = document.querySelector(buttonId);
        if (!prevButton) return;

        const pageNumbersContainer = document.createElement('div');
        pageNumbersContainer.id = containerId.substring(1);
        pageNumbersContainer.className = 'page-numbers-container';
        pageNumbersContainer.style.minWidth = '200px';

        prevButton.parentNode.insertBefore(pageNumbersContainer, prevButton.nextSibling);
    },

    // Настройка пагинации
    setupPagination() {
        this.updatePageNumbers();
        this.setupNavigationButtons();
        this.updatePageInfo();
        this.updateNavigationButtons();
    },

    // Обновить номера страниц
    updatePageNumbers() {
        this.updateSinglePageNumbers('top');
        this.updateSinglePageNumbers('bottom');
    },

    // Обновить номера страниц для одной пагинации
    updateSinglePageNumbers(position) {
        const containerId = position === 'top' ?
            this.config.pageNumbersContainerTopId :
            this.config.pageNumbersContainerBottomId;

        const pageNumbersContainer = document.querySelector(containerId);
        if (!pageNumbersContainer) return;

        pageNumbersContainer.innerHTML = '';

        // Если всего 1 страница, показываем только её
        if (this.state.totalPages <= 1) {
            this.createPageNumberButton(pageNumbersContainer, 1, position);
            return;
        }

        const { startPage, endPage } = this.getVisiblePageRange(this.config.currentPage);

        if (startPage > 1) {
            this.createPageNumberButton(pageNumbersContainer, 1, position);

            if (startPage > 2) {
                this.createEllipsis(pageNumbersContainer);
            }
        }

        for (let i = startPage; i <= endPage; i++) {
            this.createPageNumberButton(pageNumbersContainer, i, position);
        }

        if (endPage < this.state.totalPages) {
            if (endPage < this.state.totalPages - 1) {
                this.createEllipsis(pageNumbersContainer);
            }

            this.createPageNumberButton(pageNumbersContainer, this.state.totalPages, position);
        }
    },

    // Создать кнопку номера страницы
    createPageNumberButton(container, pageNumber, position) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'btn btn-sm page-number-btn';

        if (pageNumber === this.config.currentPage) {
            button.classList.add('btn-primary');
            button.title = 'Текущая страница';
        } else if (this.state.loadedPages.has(pageNumber)) {
            button.classList.add('btn-outline-primary');
            button.title = 'Загружена (кликните для показа)';
        } else {
            button.classList.add('btn-outline-secondary');
            button.title = 'Еще не загружена (кликните для загрузки)';
        }

        button.textContent = pageNumber;
        button.dataset.page = pageNumber;
        button.dataset.position = position;

        button.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();

            if (!this.state.loadedPages.has(pageNumber)) {
                this.forceLoadPage(pageNumber);
            } else {
                this.showPage(pageNumber, false);
            }
        });

        container.appendChild(button);
        return button;
    },

    // Принудительно загрузить страницу
    forceLoadPage(pageNumber) {
        // НЕ загружаем первую страницу если она уже загружена
        if (pageNumber === 1 && this.state.loadedPages.has(1)) {
            console.log('Page 1 already loaded, skipping force load');
            return Promise.resolve();
        }

        // Проверяем, не загружается ли уже эта страница
        if (this.state.loadingPages && this.state.loadingPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} is already loading, skipping duplicate request`);
            return Promise.reject(new Error(`Page ${pageNumber} is already loading`));
        }

        console.log(`Force loading page ${pageNumber}...`);

        // Обновляем состояние кнопки
        this.updatePageButtonState(pageNumber, 'loading');

        // Помечаем страницу как загружаемую
        if (!this.state.loadingPages) {
            this.state.loadingPages = new Set();
        }
        this.state.loadingPages.add(pageNumber);
        console.log(`Marked page ${pageNumber} as loading`);

        return this.loadPageFromServer(pageNumber)
            .then(games => {
                console.log(`Successfully loaded page ${pageNumber} with ${games.length} games`);

                // Обрабатываем игры
                this.processPageGames(pageNumber, games);

                // Обновляем состояние кнопки
                this.updatePageButtonState(pageNumber, 'loaded');

                // Снимаем флаг загрузки
                if (this.state.loadingPages) {
                    this.state.loadingPages.delete(pageNumber);
                    console.log(`Unmarked page ${pageNumber} as loading`);
                }

                console.log(`Page ${pageNumber} force loaded successfully`);
                return games;
            })
            .catch(error => {
                console.error(`Error force loading page ${pageNumber}:`, error);

                // Обновляем состояние кнопки при ошибке
                this.updatePageButtonState(pageNumber, 'error');

                // Снимаем флаг загрузки
                if (this.state.loadingPages) {
                    this.state.loadingPages.delete(pageNumber);
                    console.log(`Unmarked page ${pageNumber} as loading after error`);
                }

                // Убираем индикатор загрузки если он есть
                this.removeLoadingIndicator(pageNumber);

                throw error;
            });
    },

    // Проверяет, загружается ли страница в данный момент
    isLoadingPage(pageNumber) {
        return this.state.loadingPages && this.state.loadingPages.has(pageNumber);
    },

    // Помечает страницу как загружаемую
    markPageAsLoading(pageNumber) {
        if (!this.state.loadingPages) {
            this.state.loadingPages = new Set();
        }
        this.state.loadingPages.add(pageNumber);
        console.log(`Marked page ${pageNumber} as loading`);
    },

    // Снимает пометку загрузки
    unmarkPageAsLoading(pageNumber) {
        if (this.state.loadingPages && this.state.loadingPages.has(pageNumber)) {
            this.state.loadingPages.delete(pageNumber);
            console.log(`Unmarked page ${pageNumber} as loading`);
        }
    },

    // Убирает индикатор загрузки
    removeLoadingIndicator(pageNumber) {
        // Убираем конкретный индикатор
        const loadingIndicator = document.getElementById(`loading-page-${pageNumber}`);
        if (loadingIndicator) {
            console.log(`Removing loading indicator for page ${pageNumber}`);
            loadingIndicator.remove();
            return true;
        }

        // Также убираем любые другие индикаторы (на всякий случай)
        const allIndicators = document.querySelectorAll('.page-loading-indicator');
        if (allIndicators.length > 0) {
            console.log(`Removing ${allIndicators.length} orphaned loading indicators`);
            allIndicators.forEach(indicator => {
                indicator.remove();
            });
            return true;
        }

        console.log(`No loading indicator found for page ${pageNumber}`);
        return false;
    },

    // Загрузить страницу с сервера
    loadPageFromServer(pageNumber) {
        return new Promise((resolve, reject) => {
            const attemptLoad = () => {
                // Используем тот же URL что и для основной страницы
                const url = new URL(window.location.href);

                // Удаляем старые параметры пагинации
                url.searchParams.delete('page');
                url.searchParams.delete('_ajax');
                url.searchParams.delete('_scroll');

                // Добавляем новые параметры
                url.searchParams.set('page', pageNumber);
                url.searchParams.set('_ajax', '1');
                url.searchParams.set('_force', '1'); // Добавляем флаг принудительной загрузки

                console.log(`Loading page ${pageNumber} from: ${url.toString()}`);

                fetch(url.toString())
                    .then(response => {
                        if (!response.ok) {
                            throw new Error(`HTTP error! status: ${response.status}`);
                        }
                        return response.text();
                    })
                    .then(html => {
                        console.log(`Received HTML for page ${pageNumber}, length: ${html.length}`);

                        if (!html || html.trim().length === 0) {
                            reject(new Error('Empty response from server'));
                            return;
                        }

                        const parser = new DOMParser();
                        const doc = parser.parseFromString(html, 'text/html');

                        // Ищем контейнер с играми
                        const gamesContainer = doc.querySelector('.games-container .row');
                        if (!gamesContainer) {
                            console.warn('Games container not found in response');
                            // Попробуем найти игры другим способом
                            const allGames = doc.querySelectorAll('.game-card-container');
                            if (allGames.length > 0) {
                                const games = Array.from(allGames).map(el => {
                                    const cloned = el.cloneNode(true);
                                    cloned.dataset.page = pageNumber;
                                    return cloned.outerHTML;
                                });
                                console.log(`Found ${games.length} games via alternative selector`);
                                resolve(games);
                                return;
                            }
                            reject(new Error('Games container not found'));
                            return;
                        }

                        // Получаем игры из контейнера
                        const gameElements = gamesContainer.querySelectorAll('.game-card-container');

                        if (gameElements.length > 0) {
                            const games = Array.from(gameElements).map(el => {
                                const cloned = el.cloneNode(true);
                                cloned.dataset.page = pageNumber;
                                cloned.classList.add('loaded-via-ajax');
                                return cloned.outerHTML;
                            });

                            console.log(`Got ${games.length} games for page ${pageNumber}`);
                            resolve(games);
                        } else {
                            console.warn('No game elements found in container');

                            // Посмотрим что есть в контейнере
                            console.log(`Container HTML: ${gamesContainer.innerHTML.substring(0, 300)}`);

                            reject(new Error('No games found in response'));
                        }
                    })
                    .catch(error => {
                        console.error(`Error loading page ${pageNumber}:`, error);
                        reject(error);
                    });
            };

            attemptLoad();
        });
    },

    // Обработать игры страницы
    processPageGames(pageNumber, games) {
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;

        console.log(`Processing page ${pageNumber}, start index: ${startIndex}, games to process: ${games.length}`);

        // Убедимся, что gameItems существует
        if (!Array.isArray(this.state.gameItems)) {
            this.state.gameItems = [];
        }

        games.forEach((gameHtml, index) => {
            const gameIndex = startIndex + index;

            const tempDiv = document.createElement('div');
            tempDiv.innerHTML = gameHtml;
            const gameElement = tempDiv.firstChild;

            if (gameElement) {
                // Устанавливаем правильные атрибуты
                gameElement.dataset.page = pageNumber;
                gameElement.dataset.index = gameIndex;
                gameElement.style.display = 'none'; // Скрываем по умолчанию

                // Сохраняем в state
                this.state.gameItems[gameIndex] = gameElement;

                console.log(`Processed game at index ${gameIndex} for page ${pageNumber}`);
            }
        });

        this.state.loadedPages.add(pageNumber);
        this.updatePageButtonState(pageNumber, 'loaded');
        console.log(`Page ${pageNumber} processed (${games.length} games), total loaded pages: ${this.state.loadedPages.size}`);
    },

    // Сохранить страницу в хранилище
    savePageToStorage(pageNumber, games) {
        try {
            const key = `page_${pageNumber}_games`;
            sessionStorage.setItem(key, JSON.stringify(games));
        } catch (error) {
            console.error(`Error saving page ${pageNumber} to storage:`, error);
        }
    },

    // Получить страницу из хранилища
    getPageFromStorage(pageNumber) {
        try {
            const key = `page_${pageNumber}_games`;
            const data = sessionStorage.getItem(key);
            if (data) {
                return JSON.parse(data);
            }
        } catch (error) {
            console.error(`Error reading page ${pageNumber} from storage:`, error);
        }
        return null;
    },

    // Создать элемент многоточия
    createEllipsis(container) {
        const span = document.createElement('span');
        span.className = 'text-muted mx-1';
        span.textContent = '...';
        container.appendChild(span);
    },

    // Получить диапазон видимых страниц
    getVisiblePageRange(currentPage) {
        if (this.state.totalPages <= this.config.maxVisiblePages) {
            return { startPage: 1, endPage: this.state.totalPages };
        }

        let startPage = currentPage - Math.floor(this.config.maxVisiblePages / 2);
        let endPage = currentPage + Math.floor(this.config.maxVisiblePages / 2);

        if (startPage < 1) {
            startPage = 1;
            endPage = this.config.maxVisiblePages;
        }

        if (endPage > this.state.totalPages) {
            endPage = this.state.totalPages;
            startPage = endPage - this.config.maxVisiblePages + 1;
        }

        return { startPage, endPage };
    },

    // Настройка кнопок навигации
    setupNavigationButtons() {
        this.setupNavigationButtonPair('top');
        this.setupNavigationButtonPair('bottom');
    },

    // Настройка пары кнопок (prev/next)
    setupNavigationButtonPair(position) {
        const prevBtnId = position === 'top' ? this.config.prevButtonTopId : this.config.prevButtonBottomId;
        const nextBtnId = position === 'top' ? this.config.nextButtonTopId : this.config.nextButtonBottomId;

        const prevBtn = document.querySelector(prevBtnId);
        const nextBtn = document.querySelector(nextBtnId);

        if (prevBtn) {
            const newPrevBtn = prevBtn.cloneNode(true);
            prevBtn.parentNode.replaceChild(newPrevBtn, prevBtn);

            newPrevBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                if (this.config.currentPage > 1) {
                    const prevPage = this.config.currentPage - 1;
                    if (!this.state.loadedPages.has(prevPage)) {
                        this.forceLoadPage(prevPage);
                    } else {
                        this.showPage(prevPage, false);
                    }
                }
            });
        }

        if (nextBtn) {
            const newNextBtn = nextBtn.cloneNode(true);
            nextBtn.parentNode.replaceChild(newNextBtn, nextBtn);

            newNextBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                if (this.config.currentPage < this.state.totalPages) {
                    const nextPage = this.config.currentPage + 1;
                    if (!this.state.loadedPages.has(nextPage)) {
                        this.forceLoadPage(nextPage);
                    } else {
                        this.showPage(nextPage, false);
                    }
                }
            });
        }
    },

    // Показать определенную страницу
    showPage(pageNumber, isBackground = false) {
        if (pageNumber < 1 || pageNumber > this.state.totalPages) {
            console.log(`Page ${pageNumber} out of range (1-${this.state.totalPages})`);
            return;
        }

        console.log(`showPage called for page ${pageNumber}, isBackground: ${isBackground}, already loaded: ${this.state.loadedPages.has(pageNumber)}`);

        // Проверяем, загружена ли страница
        if (this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loaded, showing directly`);

            // ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: убедимся, что в DOM правильные игры
            if (!this.verifyDOMGames(pageNumber)) {
                console.log(`DOM verification failed for page ${pageNumber}, reloading...`);
                this.state.loadedPages.delete(pageNumber);
                this.showPage(pageNumber, isBackground);
                return;
            }

            this._showPageAfterLoad(pageNumber);
            return;
        }

        // Проверяем, загружается ли страница
        if (this.state.loadingPages && this.state.loadingPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} is already loading, waiting...`);
            return;
        }

        // Очищаем DOM
        this.clearDOMContainer();

        // Показываем индикатор загрузки если это не фоновая загрузка
        if (!isBackground) {
            console.log(`Page ${pageNumber} not loaded, showing indicator`);
            this.showLoadingIndicator(pageNumber);
        }

        console.log(`Page ${pageNumber} not loaded, loading now...`);
        this.forceLoadPage(pageNumber).then(() => {
            // После успешной загрузки показываем страницу
            if (!isBackground) {
                this._showPageAfterLoad(pageNumber);
            }
        }).catch(error => {
            console.error(`Error loading page ${pageNumber}:`, error);
            // Убираем индикатор при ошибке
            this.removeLoadingIndicator(pageNumber);
        });
    },

    // Очистить DOM контейнер
    clearDOMContainer() {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (rowElement) {
            // Сохраняем только скрытые поля и отладочную информацию
            const hiddenElements = rowElement.querySelectorAll('input[type="hidden"], #server-debug-info, #extended-debug-info');
            const hiddenElementsHTML = Array.from(hiddenElements).map(el => el.outerHTML).join('');

            rowElement.innerHTML = hiddenElementsHTML;
            console.log('Cleared DOM container, kept hidden fields');
        }
    },

    cacheGamesFromDOM(pageNumber) {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return;

        const gameElements = rowElement.querySelectorAll('.game-card-container');

        // Рассчитываем начальный индекс для страницы
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;

        // Сохраняем игры в кэш
        gameElements.forEach((element, index) => {
            const gameIndex = startIndex + index;
            if (!this.state.gameItems[gameIndex]) {
                this.state.gameItems[gameIndex] = element;
                element.dataset.page = pageNumber;
                console.log(`Cached game at index ${gameIndex} for page ${pageNumber} from DOM`);
            }
        });
    },

    // Внутренний метод для отображения страницы после загрузки
    _showPageAfterLoad(pageNumber) {
        console.log(`_showPageAfterLoad for page ${pageNumber}, current gameItems length: ${this.state.gameItems.length}`);

        // Убираем индикатор загрузки если он есть
        this.removeLoadingIndicator(pageNumber);

        this.config.currentPage = pageNumber;

        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;
        const endIndex = Math.min(startIndex + this.config.itemsPerPage, this.state.totalItems);

        console.log(`Showing page ${pageNumber}: items ${startIndex + 1}-${endIndex} (indices ${startIndex}-${endIndex-1})`);

        // Получаем контейнер
        const container = document.querySelector(this.config.containerSelector);
        if (!container) {
            console.error('Games container not found');
            return;
        }

        // Находим или создаем row элемент
        let rowElement = container.querySelector('.row');
        if (!rowElement) {
            rowElement = document.createElement('div');
            rowElement.className = 'row';
            container.appendChild(rowElement);
        }

        // Очищаем row элемент (но сохраняем скрытые поля)
        const hiddenElements = rowElement.querySelectorAll('input[type="hidden"], #server-debug-info, #extended-debug-info');
        const hiddenElementsHTML = Array.from(hiddenElements).map(el => el.outerHTML).join('');
        rowElement.innerHTML = hiddenElementsHTML;

        // Добавляем игры для текущей страницы в row
        let gamesAdded = 0;
        let missingGames = 0;

        for (let i = startIndex; i < endIndex; i++) {
            if (this.state.gameItems[i] && this.state.gameItems[i].nodeType === Node.ELEMENT_NODE) {
                const gameElement = this.state.gameItems[i];

                // Клонируем элемент чтобы избежать проблем с перемещением
                const clonedElement = gameElement.cloneNode(true);
                clonedElement.style.display = 'block';
                clonedElement.style.animation = 'pageFadeIn 0.3s ease';
                clonedElement.dataset.page = pageNumber;
                clonedElement.dataset.index = i;

                rowElement.appendChild(clonedElement);
                gamesAdded++;

                console.log(`Added game at index ${i} for page ${pageNumber}`);
            } else {
                // Если игры нет в кэше, создаем placeholder
                const placeholder = this.createGamePlaceholder(i + 1);
                placeholder.dataset.page = pageNumber;
                placeholder.dataset.index = i;
                rowElement.appendChild(placeholder);
                missingGames++;

                console.warn(`Missing game at index ${i} for page ${pageNumber}`);
            }
        }

        console.log(`Added ${gamesAdded} games, ${missingGames} placeholders to page ${pageNumber}`);

        // Обновляем UI
        this.updatePageNumbers();
        this.updatePageInfo();
        this.updateNavigationButtons();

        // Обновляем URL без перезагрузки страницы
        this.updateBrowserUrl(pageNumber);

        // Сохраняем в storage
        this.saveCurrentPageToStorage(pageNumber);

        // ПРЕДЗАГРУЖАЕМ соседние страницы
        this.preloadAdjacentPages(pageNumber);
    },

    // ОБНОВЛЕННЫЙ МЕТОД: Обновить URL браузера
    updateBrowserUrl(pageNumber) {
        const url = new URL(window.location.href);

        // Устанавливаем или обновляем параметр page
        if (pageNumber === 1) {
            // Для первой страницы удаляем параметр page
            url.searchParams.delete('page');
        } else {
            url.searchParams.set('page', pageNumber);
        }

        // Сохраняем в истории без перезагрузки
        window.history.replaceState({
            page: pageNumber,
            timestamp: Date.now()
        }, document.title, url.toString());

        console.log(`Updated URL to: ${url.toString()}`);
    },

    // Предзагрузка соседних страниц
    preloadAdjacentPages(currentPage) {
        const pagesToPreload = [];

        // Предзагружаем следующую страницу
        if (currentPage < this.state.totalPages) {
            const nextPage = currentPage + 1;
            if (!this.state.loadedPages.has(nextPage) &&
                !(this.state.loadingPages && this.state.loadingPages.has(nextPage))) {
                pagesToPreload.push(nextPage);
            }
        }

        // Предзагружаем предыдущую страницу
        if (currentPage > 1) {
            const prevPage = currentPage - 1;
            if (!this.state.loadedPages.has(prevPage) &&
                !(this.state.loadingPages && this.state.loadingPages.has(prevPage))) {
                pagesToPreload.push(prevPage);
            }
        }

        // Загружаем в фоне
        pagesToPreload.forEach(page => {
            console.log(`Preloading page ${page} in background...`);
            this.loadPageInBackground(page);
        });
    },

    // Загрузить страницу в фоне
    loadPageInBackground(pageNumber) {
        if (this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loaded, skipping background load`);
            return;
        }

        // Проверяем, не загружается ли уже
        if (this.state.loadingPages && this.state.loadingPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loading, skipping duplicate`);
            return;
        }

        // Сначала проверяем кэш
        const cachedGames = this.getPageFromStorage(pageNumber);
        if (cachedGames && cachedGames.length > 0) {
            console.log(`Found page ${pageNumber} in cache, processing...`);
            this.processPageGames(pageNumber, cachedGames);
            return;
        }

        // Помечаем как загружаемую
        if (!this.state.loadingPages) {
            this.state.loadingPages = new Set();
        }
        this.state.loadingPages.add(pageNumber);

        // Загружаем с сервера
        this.loadPageFromServer(pageNumber)
            .then(games => {
                this.processPageGames(pageNumber, games);
                console.log(`Page ${pageNumber} loaded in background`);

                // Снимаем флаг загрузки
                if (this.state.loadingPages) {
                    this.state.loadingPages.delete(pageNumber);
                }
            })
            .catch(error => {
                console.error(`Error loading page ${pageNumber} in background:`, error);

                // Снимаем флаг загрузки при ошибке
                if (this.state.loadingPages) {
                    this.state.loadingPages.delete(pageNumber);
                }
            });
    },

    // Создать placeholder для игры
    createGamePlaceholder(index) {
        const placeholder = document.createElement('div');
        placeholder.className = 'col-xl-3 col-lg-4 col-md-6 mb-4 game-card-container';
        placeholder.dataset.index = index;
        placeholder.innerHTML = `
            <div class="game-card placeholder">
                <div class="card h-100">
                    <div class="card-body text-center">
                        <div class="spinner-border text-primary mb-3" role="status">
                            <span class="visually-hidden">Loading...</span>
                        </div>
                        <p class="text-muted">Loading game ${index + 1}...</p>
                    </div>
                </div>
            </div>
        `;
        placeholder.style.display = 'block';
        return placeholder;
    },

    // Обновить информацию о странице
    updatePageInfo() {
        const startIndex = (this.config.currentPage - 1) * this.config.itemsPerPage + 1;
        const endIndex = Math.min(this.config.currentPage * this.config.itemsPerPage, this.state.totalItems);

        this.updateSinglePageInfo('top', startIndex, endIndex);
        this.updateSinglePageInfo('bottom', startIndex, endIndex);
    },

    updateSinglePageInfo(position, startIndex, endIndex) {
        const startElement = document.querySelector(
            position === 'top' ? this.config.startElementTopId : this.config.startElementBottomId
        );
        const endElement = document.querySelector(
            position === 'top' ? this.config.endElementTopId : this.config.endElementBottomId
        );
        const currentElement = document.querySelector(
            position === 'top' ? this.config.currentElementTopId : this.config.currentElementBottomId
        );
        const totalElement = document.querySelector(
            position === 'top' ? this.config.totalElementTopId : this.config.totalElementBottomId
        );
        const totalPagesElement = document.querySelector(
            position === 'top' ? this.config.totalPagesElementTopId : this.config.totalPagesElementBottomId
        );

        if (startElement) startElement.textContent = startIndex;
        if (endElement) endElement.textContent = endIndex;
        if (currentElement) currentElement.textContent = this.config.currentPage;
        if (totalElement) totalElement.textContent = this.state.totalItems;
        if (totalPagesElement) totalPagesElement.textContent = this.state.totalPages;
    },

    // Обновить кнопки навигации
    updateNavigationButtons() {
        this.updateNavigationButtonPair('top');
        this.updateNavigationButtonPair('bottom');
    },

    updateNavigationButtonPair(position) {
        const prevBtnId = position === 'top' ? this.config.prevButtonTopId : this.config.prevButtonBottomId;
        const nextBtnId = position === 'top' ? this.config.nextButtonTopId : this.config.nextButtonBottomId;

        const prevBtn = document.querySelector(prevBtnId);
        const nextBtn = document.querySelector(nextBtnId);

        if (prevBtn) {
            if (this.config.currentPage === 1 || this.state.totalPages <= 1) {
                prevBtn.classList.add('disabled');
                prevBtn.disabled = true;
            } else {
                prevBtn.classList.remove('disabled');
                prevBtn.disabled = false;
            }
        }

        if (nextBtn) {
            if (this.config.currentPage === this.state.totalPages || this.state.totalPages <= 1) {
                nextBtn.classList.add('disabled');
                nextBtn.disabled = true;
            } else {
                nextBtn.classList.remove('disabled');
                nextBtn.disabled = false;
            }
        }
    },

    // Обновить состояние кнопки страницы
    updatePageButtonState(pageNumber, state) {
        const buttons = document.querySelectorAll(`.page-number-btn[data-page="${pageNumber}"]`);
        buttons.forEach(button => {
            if (state === 'loading') {
                button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
                button.disabled = true;
                button.classList.remove('btn-outline-primary', 'btn-outline-secondary', 'btn-outline-danger');
                button.classList.add('btn-secondary');
                button.title = 'Загрузка...';
            } else if (state === 'loaded') {
                button.textContent = pageNumber;
                button.disabled = false;
                button.classList.remove('btn-secondary', 'btn-outline-secondary', 'btn-outline-danger');
                button.classList.add('btn-outline-primary');
                button.title = 'Загружена (кликните для показа)';
            } else if (state === 'current') {
                button.textContent = pageNumber;
                button.classList.remove('btn-outline-primary', 'btn-outline-secondary', 'btn-secondary', 'btn-outline-danger');
                button.classList.add('btn-primary');
                button.title = 'Текущая страница';
            } else if (state === 'not-loaded') {
                button.textContent = pageNumber;
                button.disabled = false;
                button.classList.remove('btn-primary', 'btn-outline-primary', 'btn-secondary', 'btn-outline-danger');
                button.classList.add('btn-outline-secondary');
                button.title = 'Еще не загружена (кликните для загрузки)';
            } else if (state === 'error') {
                button.textContent = pageNumber;
                button.disabled = false;
                button.classList.remove('btn-primary', 'btn-outline-primary', 'btn-secondary');
                button.classList.add('btn-outline-danger');
                button.title = 'Ошибка загрузки (попробуйте снова)';
            }
        });
    },

    // Скрыть пагинацию
    hidePagination() {
        const topContainer = document.querySelector(this.config.paginationTopSelector);
        const bottomContainer = document.querySelector(this.config.paginationBottomSelector);

        if (topContainer) {
            topContainer.style.display = 'none';
        }
        if (bottomContainer) {
            bottomContainer.style.display = 'none';
        }
    },

    // Показать пагинацию
    showPagination() {
        const topContainer = document.querySelector(this.config.paginationTopSelector);
        const bottomContainer = document.querySelector(this.config.paginationBottomSelector);

        if (topContainer) {
            topContainer.style.display = 'block';
        }
        if (bottomContainer) {
            bottomContainer.style.display = 'block';
        }
    },

    // Обновить пагинацию после изменений
    updateAfterChanges() {
        console.log('Updating lazy pagination after changes...');

        this.loadPaginationInfoFromDOM();

        this.showPagination();

        if (!document.querySelector(this.config.pageNumbersContainerTopId)) {
            this.createPageNumbersContainer('top');
        }
        if (!document.querySelector(this.config.pageNumbersContainerBottomId)) {
            this.createPageNumbersContainer('bottom');
        }

        this.setupPagination();

        // Восстанавливаем страницу из URL
        const initialPage = this.loadCurrentPageFromURL();

        // Проверяем, загружена ли страница
        if (this.state.loadedPages.has(initialPage)) {
            this.showPage(initialPage, false);
        } else if (initialPage > 1) {
            // Показываем индикатор и загружаем
            this.showLoadingIndicator(initialPage);
            this.forceLoadPage(initialPage).then(() => {
                this.showPage(initialPage, false);
            }).catch(() => {
                this.showPage(1, false);
            });
        } else {
            this.showPage(initialPage, false);
        }
    },

    // Сбросить к первой странице
    resetToFirstPage() {
        this.clearPageState(); // Очищаем сохраненное состояние
        this.showPage(1, false);
    },

    // Принудительно обновить
    forceUpdate() {
        console.log('Force updating lazy pagination...');

        this.loadPaginationInfoFromDOM();
        this.loadCurrentGamesFromDOM();

        console.log(`Force update: ${this.state.totalItems} items, ${this.state.totalPages} pages`);

        this.showPagination();

        this.createPageNumbersContainer('top');
        this.createPageNumbersContainer('bottom');

        this.setupPagination();

        // Восстанавливаем страницу из URL
        const initialPage = this.loadCurrentPageFromURL();

        // Если страница не загружена, загружаем её
        if (!this.state.loadedPages.has(initialPage)) {
            if (initialPage > 1) {
                console.log(`Force update: loading page ${initialPage}`);
                this.forceLoadPage(initialPage).then(() => {
                    this.showPage(initialPage, false);
                }).catch(() => {
                    this.showPage(1, false);
                });
            } else {
                this.showPage(initialPage, false);
            }
        } else {
            this.showPage(initialPage, false);
        }
    },

    // Получить текущую страницу
    getCurrentPage() {
        return this.config.currentPage;
    },

    // Получить общее количество страниц
    getTotalPages() {
        return this.state.totalPages;
    },

    // Проверить есть ли пагинация
    hasPagination() {
        return this.state.totalPages > 1;
    },

    // Запустить фоновую загрузку всех страниц
    startBackgroundLoading() {
        console.log('Starting background page loading...');

        // Загружаем следующую страницу сразу
        if (this.state.totalPages > 1) {
            this.loadPageInBackground(2);
        }

        // Остальные страницы загружаем с задержкой
        for (let page = 3; page <= this.state.totalPages; page++) {
            setTimeout(() => {
                this.loadPageInBackground(page);
            }, (page - 2) * 2000);
        }
    },

    // Деструктор для очистки
    destroy() {
        console.log('Destroying lazy game pagination...');
        this.state.gameItems = [];
        this.state.loadedPages.clear();
        this.clearPageState();
    }
};

document.addEventListener('DOMContentLoaded', () => {
    setTimeout(() => {
        console.log('DOM loaded, initializing lazy game pagination...');
        GamePagination.init();
    }, 500);
});

window.addEventListener('load', () => {
    setTimeout(() => {
        console.log('Page fully loaded, checking lazy game pagination...');
        if (!GamePagination.state.gameItems || GamePagination.state.gameItems.length === 0) {
            GamePagination.forceUpdate();
        }

        // Запускаем фоновую загрузку после полной загрузки страницы
        setTimeout(() => {
            if (GamePagination.hasPagination()) {
                GamePagination.startBackgroundLoading();
            }
        }, 2000);
    }, 1000);
});

// ОБРАБОТЧИК ДЛЯ ИЗМЕНЕНИЯ ФИЛЬТРОВ
document.addEventListener('filterApplied', () => {
    console.log('Filter applied, resetting pagination to page 1...');
    GamePagination.resetToFirstPage();
});

export default GamePagination;