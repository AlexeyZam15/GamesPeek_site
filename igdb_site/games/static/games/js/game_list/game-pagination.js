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
        console.log('=== LAZY GAMES PAGINATION INITIALIZATION OPTIMIZED ===');

        this.state.gameItems = [];
        this.state.loadedPages.clear();

        if (!this.state.loadingPages) {
            this.state.loadingPages = new Set();
        }

        this.loadPaginationInfoFromDOM();

        if (this.state.totalItems === 0) {
            console.error('ERROR: No items found for pagination!');
            this.hidePagination();
            return;
        }

        const urlPage = this.getPageFromURL();
        console.log(`URL requests page: ${urlPage}`);

        this.config.currentPage = urlPage;

        if (this.state.totalPages > 0 && this.config.currentPage > this.state.totalPages) {
            console.log(`Adjusting page ${this.config.currentPage} to ${this.state.totalPages}`);
            this.config.currentPage = Math.max(1, this.state.totalPages);
        }

        console.log(`Initial page set to: ${this.config.currentPage}`);

        const container = document.querySelector(this.config.containerSelector);
        const rowElement = container ? container.querySelector('.row') : null;
        const existingGames = rowElement ? rowElement.querySelectorAll('.game-card-container') : [];

        const hasCorrectGamesInDOM = this.arePageGamesInDOM(this.config.currentPage);

        if (hasCorrectGamesInDOM && existingGames.length > 0) {
            console.log(`✅ Page ${this.config.currentPage} already has ${existingGames.length} games in DOM, skipping reload`);

            this.cacheGamesFromDOM(this.config.currentPage);
            this.state.loadedPages.add(this.config.currentPage);

            this.createPageNumbersContainer('top');
            this.createPageNumbersContainer('bottom');

            this.setupPagination();

            this.updatePageNumbers();
            this.updatePageInfo();
            this.updateNavigationButtons();

            this.saveFirstPageToLocalCache();

            this.preloadAdjacentPages(this.config.currentPage);

            return;
        }

        console.log(`🔄 No correct games in DOM for page ${this.config.currentPage}, loading...`);

        this.createPageNumbersContainer('top');
        this.createPageNumbersContainer('bottom');

        this.setupPagination();

        if (this.config.currentPage === 1) {
            const cachedFirstPage = this.loadFirstPageFromLocalCache();
            if (cachedFirstPage && cachedFirstPage.length > 0) {
                console.log(`✅ Loaded page 1 from browser cache: ${cachedFirstPage.length} games`);
                this.processPageGames(1, cachedFirstPage);
                this.showPageFromCache(1);
                return;
            }
        }

        this.checkAndCacheInitialPage();
    },

    // Получить или загрузить страницу с кэшированием
    getOrLoadPageWithCache(pageNumber) {
        if (pageNumber < 1 || pageNumber > this.state.totalPages) {
            console.log(`Page ${pageNumber} out of range (1-${this.state.totalPages})`);
            return Promise.reject(new Error('Page out of range'));
        }

        console.log(`getOrLoadPageWithCache for page ${pageNumber}`);

        // 1. Проверяем, загружена ли страница в памяти
        if (this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loaded in memory`);
            return Promise.resolve(this._getPageFromMemory(pageNumber));
        }

        // 2. Проверяем локальный кэш браузера (для первой страницы)
        if (pageNumber === 1) {
            const cachedFirstPage = this.loadFirstPageFromLocalCache();
            if (cachedFirstPage && cachedFirstPage.length > 0) {
                console.log(`Loading page 1 from browser cache`);
                this.processPageGames(1, cachedFirstPage);
                return Promise.resolve(cachedFirstPage);
            }
        }

        // 3. Проверяем sessionStorage кэш
        const sessionCacheKey = `page_${pageNumber}_cached`;
        try {
            const cachedData = sessionStorage.getItem(sessionCacheKey);
            if (cachedData) {
                const data = JSON.parse(cachedData);

                // Проверяем актуальность (не старше 10 минут)
                const maxAge = 10 * 60 * 1000;
                if (Date.now() - data.timestamp < maxAge) {
                    console.log(`Loading page ${pageNumber} from session cache`);
                    this.processPageGames(pageNumber, data.games);
                    return Promise.resolve(data.games);
                }
            }
        } catch (e) {
            console.warn(`Error reading session cache for page ${pageNumber}:`, e);
        }

        // 4. Загружаем с сервера
        console.log(`Loading page ${pageNumber} from server...`);
        return this.forceLoadPage(pageNumber)
            .then(games => {
                // Сохраняем в sessionStorage
                try {
                    const cacheData = {
                        games: games,
                        timestamp: Date.now(),
                        page: pageNumber
                    };
                    sessionStorage.setItem(sessionCacheKey, JSON.stringify(cacheData));
                    console.log(`Cached page ${pageNumber} in sessionStorage`);
                } catch (e) {
                    console.warn(`Could not cache page ${pageNumber} in sessionStorage:`, e);
                }

                return games;
            });
    },

    // Получить страницу из памяти
    _getPageFromMemory(pageNumber) {
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;
        const endIndex = Math.min(startIndex + this.config.itemsPerPage, this.state.totalItems);

        const games = [];
        for (let i = startIndex; i < endIndex; i++) {
            if (this.state.gameItems[i]) {
                games.push(this.state.gameItems[i].outerHTML);
            }
        }

        return games;
    },

    // Загрузить первую страницу из локального кэша браузера
    loadFirstPageFromLocalCache() {
        try {
            const cachedData = localStorage.getItem('gamepagination_firstpage_cache');
            if (!cachedData) return null;

            const data = JSON.parse(cachedData);

            // Проверяем актуальность (не старше 30 минут)
            const maxAge = 30 * 60 * 1000; // 30 минут
            if (Date.now() - data.timestamp > maxAge) {
                localStorage.removeItem('gamepagination_firstpage_cache');
                return null;
            }

            // Проверяем, что это тот же URL
            if (data.url !== window.location.href) {
                localStorage.removeItem('gamepagination_firstpage_cache');
                return null;
            }

            console.log(`Found cached page 1 in browser cache (${data.html.length} games)`);
            return data.html;
        } catch (error) {
            console.warn('Could not load first page from cache:', error);
            localStorage.removeItem('gamepagination_firstpage_cache');
            return null;
        }
    },

    // Сохранить первую страницу в локальный кэш браузера
    saveFirstPageToLocalCache() {
        try {
            if (this.config.currentPage !== 1) return;

            const container = document.querySelector(this.config.containerSelector);
            if (!container) return;

            const rowElement = container.querySelector('.row');
            if (!rowElement) return;

            const gameElements = rowElement.querySelectorAll('.game-card-container');
            if (gameElements.length === 0) return;

            const gamesHTML = Array.from(gameElements).map(el => el.outerHTML);

            const cacheData = {
                html: gamesHTML,
                timestamp: Date.now(),
                totalItems: this.state.totalItems,
                totalPages: this.state.totalPages,
                url: window.location.href
            };

            localStorage.setItem('gamepagination_firstpage_cache', JSON.stringify(cacheData));
            console.log(`Saved page 1 to browser cache: ${gamesHTML.length} games`);
        } catch (error) {
            console.warn('Could not save first page to cache:', error);
        }
    },

    // Проверить, есть ли игры страницы в DOM
    arePageGamesInDOM(pageNumber) {
        if (!pageNumber || pageNumber < 1) return false;

        const container = document.querySelector(this.config.containerSelector);
        if (!container) return false;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return false;

        const gameElements = rowElement.querySelectorAll('.game-card-container');
        const gameCount = gameElements.length;

        // Если нет игр в DOM
        if (gameCount === 0) {
            console.log(`No games in DOM for page check`);
            return false;
        }

        // Проверяем data-page атрибуты
        let allGamesMatch = true;
        let firstGamePage = null;

        for (const gameElement of gameElements) {
            const gamePage = parseInt(gameElement.dataset.page);

            if (isNaN(gamePage)) {
                // Если у игры нет data-page, считаем что это может быть правильная страница
                continue;
            }

            if (firstGamePage === null) {
                firstGamePage = gamePage;
            }

            if (gamePage !== pageNumber) {
                console.log(`Game has data-page="${gamePage}", expected ${pageNumber}`);
                allGamesMatch = false;
            }
        }

        // Если все игры имеют правильный data-page
        if (allGamesMatch && firstGamePage === pageNumber) {
            console.log(`All ${gameCount} games in DOM have correct data-page="${pageNumber}"`);
            return true;
        }

        // Если некоторые игры не имеют data-page, но количество игр соответствует странице
        const expectedCount = (pageNumber === this.state.totalPages)
            ? (this.state.totalItems % this.config.itemsPerPage || this.config.itemsPerPage)
            : this.config.itemsPerPage;

        if (gameCount === expectedCount || gameCount > 0) {
            console.log(`Found ${gameCount} games in DOM, likely for page ${pageNumber}`);
            return true;
        }

        return false;
    },

    // Проверить и кэшировать начальную страницу из DOM
    checkAndCacheInitialPage() {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return;

        const existingGames = rowElement.querySelectorAll('.game-card-container');
        console.log(`Found ${existingGames.length} games in DOM on init`);

        if (existingGames.length === 0) {
            const noGamesElement = rowElement.querySelector('.text-center.py-5');
            if (noGamesElement && noGamesElement.textContent.includes('No games found')) {
                console.log('Found "No games found" message, this is really an empty page');
                this.hidePagination();
                return;
            }

            console.log(`No games in DOM, loading page ${this.config.currentPage}`);
            this.showPage(this.config.currentPage, false);
            return;
        }

        // Определяем для какой страницы эти игры
        let pageToCache = this.config.currentPage;
        const gamePages = new Set();

        for (const game of existingGames) {
            const page = parseInt(game.dataset.page);
            if (!isNaN(page) && page > 0) {
                gamePages.add(page);
            }
        }

        if (gamePages.size === 1) {
            const detectedPage = Array.from(gamePages)[0];
            console.log(`All games have data-page="${detectedPage}"`);

            if (detectedPage !== this.config.currentPage) {
                console.log(`DOM has page ${detectedPage}, but URL says ${this.config.currentPage}. Using DOM page.`);
                pageToCache = detectedPage;
                this.config.currentPage = detectedPage;
            }
        } else if (gamePages.size === 0) {
            const gamesCount = existingGames.length;
            const itemsPerPage = this.config.itemsPerPage;

            if (gamesCount === itemsPerPage) {
                console.log(`Full page of ${gamesCount} games detected, using URL page: ${this.config.currentPage}`);
            } else if (gamesCount < itemsPerPage && this.config.currentPage === this.state.totalPages) {
                console.log(`Partial page (${gamesCount} games) detected, assuming last page: ${this.config.currentPage}`);
            }
        }

        const noGamesMsg = rowElement.querySelector('.text-center.py-5');
        if (noGamesMsg && noGamesMsg.textContent.includes('No games found')) {
            console.log('Removing "No games found" message');
            noGamesMsg.remove();
        }

        // Кэшируем игры из DOM
        this.cacheGamesFromDOM(pageToCache);
        this.state.loadedPages.add(pageToCache);

        console.log(`Cached ${existingGames.length} games from DOM for page ${pageToCache}`);

        // Сохраняем в локальный кэш если это первая страница
        if (pageToCache === 1) {
            this.saveFirstPageToLocalCache();
        }

        // Обновляем UI
        this.updatePageNumbers();
        this.updatePageInfo();
        this.updateNavigationButtons();

        console.log(`Initial page ${pageToCache} loaded from DOM`);
    },

    // Определить, нужно ли обновлять URL при смене страницы
    shouldUpdateUrlOnPageChange(pageNumber) {
        // Если это первая страница и в URL нет параметра page, не обновляем
        const urlParams = new URLSearchParams(window.location.search);
        const currentPageInUrl = urlParams.get('page');

        if (pageNumber === 1 && !currentPageInUrl) {
            return false;
        }

        // Если страница уже указана в URL, не обновляем
        if (currentPageInUrl && parseInt(currentPageInUrl) === pageNumber) {
            return false;
        }

        return true;
    },

    // Показать страницу из кеша (уже загруженную)
    showPageFromCache(pageNumber) {
        console.log(`showPageFromCache for page ${pageNumber}`);

        this.removeLoadingIndicator(pageNumber);

        this.config.currentPage = pageNumber;

        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;
        const endIndex = Math.min(startIndex + this.config.itemsPerPage, this.state.totalItems);

        console.log(`Showing cached page ${pageNumber}: items ${startIndex + 1}-${endIndex}`);

        const container = document.querySelector(this.config.containerSelector);
        if (!container) {
            console.error('Games container not found');
            return;
        }

        let rowElement = container.querySelector('.row');
        if (!rowElement) {
            rowElement = document.createElement('div');
            rowElement.className = 'row';
            container.appendChild(rowElement);
        }

        // Очищаем только если есть неправильные игры
        let needsUpdate = true;
        const existingGames = rowElement.querySelectorAll('.game-card-container');

        if (existingGames.length > 0) {
            needsUpdate = false;
            for (const game of existingGames) {
                const gamePage = parseInt(game.dataset.page);
                if (gamePage !== pageNumber) {
                    needsUpdate = true;
                    break;
                }
            }
        }

        if (needsUpdate) {
            console.log(`Updating DOM for page ${pageNumber}`);

            // Используем DocumentFragment для быстрой вставки
            const fragment = document.createDocumentFragment();
            let gamesAdded = 0;

            for (let i = startIndex; i < endIndex; i++) {
                if (this.state.gameItems[i] && this.state.gameItems[i].nodeType === Node.ELEMENT_NODE) {
                    const gameElement = this.state.gameItems[i];

                    const clonedElement = gameElement.cloneNode(true);
                    clonedElement.style.display = 'block';
                    clonedElement.style.animation = 'pageFadeIn 0.3s ease';
                    clonedElement.dataset.page = pageNumber;
                    clonedElement.dataset.index = i;

                    fragment.appendChild(clonedElement);
                    gamesAdded++;
                }
            }

            // Быстрая замена содержимого
            rowElement.innerHTML = '';
            rowElement.appendChild(fragment);

            console.log(`Added ${gamesAdded} games from cache to page ${pageNumber}`);
        } else {
            console.log(`DOM already has correct games for page ${pageNumber}, skipping update`);
        }

        this.updatePageNumbers();
        this.updatePageInfo();
        this.updateNavigationButtons();

        if (this.shouldUpdateUrlOnPageChange(pageNumber)) {
            this.updateBrowserUrl(pageNumber);
        }

        this.saveCurrentPageToStorage(pageNumber);

        this.preloadAdjacentPages(pageNumber);
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

            console.log(`Removed ${keysToRemove.length} sessionStorage cache keys`);
        } catch (e) {
            console.warn('Error clearing sessionStorage:', e);
        }

        // Очищаем localStorage
        try {
            localStorage.removeItem('gamepagination_firstpage_cache');
            console.log('Removed localStorage first page cache');
        } catch (e) {
            console.warn('Error clearing localStorage:', e);
        }

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

    // Получить номер страницы из URL
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

        // ВАЖНО: Если в URL нет параметра page, значит это первая страница
        console.log(`No page in URL, defaulting to page 1`);
        return 1;
    },

    // Показать индикатор загрузки
    showLoadingIndicator(pageNumber) {
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

        // НЕ очищаем контейнер! Добавляем индикатор поверх существующих игр
        loadingDiv.style.position = 'absolute';
        loadingDiv.style.zIndex = '1000';

        // Устанавливаем position: relative для row элемента
        rowElement.style.position = 'relative';

        // Добавляем индикатор
        rowElement.appendChild(loadingDiv);

        // Обновляем информацию о странице
        this.updatePageNumbers();
        this.updatePageInfo();
        this.updateNavigationButtons();
    },

    // Сохранить страницу в storage
    saveCurrentPageToStorage(pageNumber) {
        try {
            const state = {
                page: pageNumber,
                timestamp: Date.now(),
                url: window.location.href
            };
            sessionStorage.setItem('gamePaginationState', JSON.stringify(state));
            console.log(`Saved page ${pageNumber} to storage, URL: ${window.location.href}`);
        } catch (e) {
            console.warn('Could not save page to storage:', e);
        }
    },

    // Восстановить страницу из storage
    restorePageFromStorage() {
        try {
            const saved = sessionStorage.getItem('gamePaginationState');
            if (saved) {
                const state = JSON.parse(saved);

                // Проверяем актуальность (не старше 5 минут)
                const age = Date.now() - state.timestamp;
                const maxAge = 5 * 60 * 1000; // 5 минут

                // ВАЖНО: Проверяем, что это тот же URL (включая параметры)
                if (age < maxAge && state.url === window.location.href) {
                    console.log(`Restored page ${state.page} from storage for URL: ${state.url}`);
                    return state.page;
                } else {
                    // Удаляем устаревшие данные
                    sessionStorage.removeItem('gamePaginationState');
                    console.log('Removed expired storage data');
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
        console.log('Loading pagination info from DOM...');

        // Сначала пробуем получить из скрытых полей сервера
        const serverTotalElement = document.getElementById('server-total-count');
        const serverTotalPagesElement = document.getElementById('server-total-pages');
        const serverItemsPerPageElement = document.getElementById('server-items-per-page');

        // Если есть серверные данные - используем их
        if (serverTotalElement && serverTotalPagesElement) {
            this.state.totalItems = parseInt(serverTotalElement.value) || 0;
            this.state.totalPages = parseInt(serverTotalPagesElement.value) || 1;

            if (serverItemsPerPageElement) {
                this.config.itemsPerPage = parseInt(serverItemsPerPageElement.value) || 16;
            }

            console.log(`Got pagination info from server fields: ${this.state.totalItems} items, ${this.state.totalPages} pages, ${this.config.itemsPerPage} items per page`);
        } else {
            // Пробуем получить из data-атрибутов контейнера
            const gamesContainer = document.querySelector(this.config.containerSelector);
            if (gamesContainer) {
                if (gamesContainer.dataset.totalPages) {
                    this.state.totalPages = parseInt(gamesContainer.dataset.totalPages) || 1;
                    console.log(`Got totalPages from data attribute: ${this.state.totalPages}`);
                }

                if (gamesContainer.dataset.totalGames) {
                    this.state.totalItems = parseInt(gamesContainer.dataset.totalGames) || 0;
                    console.log(`Got totalItems from data attribute: ${this.state.totalItems}`);
                }

                if (gamesContainer.dataset.itemsPerPage) {
                    this.config.itemsPerPage = parseInt(gamesContainer.dataset.itemsPerPage) || 16;
                    console.log(`Got itemsPerPage from data attribute: ${this.config.itemsPerPage}`);
                }
            }
        }

        // Если все еще нет данных - пробуем из видимых элементов на странице
        if (this.state.totalItems === 0) {
            const totalElement = document.querySelector(this.config.totalElementBottomId);
            if (totalElement) {
                const totalText = totalElement.textContent.trim();
                const match = totalText.match(/\d+/);
                if (match) {
                    this.state.totalItems = parseInt(match[0]) || 0;
                    console.log(`Got totalItems from visible element: ${this.state.totalItems}`);
                }
            }
        }

        if (this.state.totalPages === 0) {
            const totalPagesElement = document.querySelector(this.config.totalPagesElementBottomId);
            if (totalPagesElement) {
                const totalPagesText = totalPagesElement.textContent.trim();
                const match = totalPagesText.match(/\d+/);
                if (match) {
                    this.state.totalPages = parseInt(match[0]) || 1;
                    console.log(`Got totalPages from visible element: ${this.state.totalPages}`);
                }
            }
        }

        // Если все еще нет данных - рассчитаем сами
        if (this.state.totalItems > 0 && this.state.totalPages <= 1) {
            this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);
            console.log(`Calculated totalPages: ${this.state.totalPages} from ${this.state.totalItems} items with ${this.config.itemsPerPage} per page`);
        }

        // Минимальная проверка
        if (this.state.totalItems === 0) {
            console.warn('WARNING: No items found for pagination');

            // Проверяем, есть ли сообщение "No games found" в DOM
            const noGamesElement = document.querySelector('.text-center.py-5');
            if (noGamesElement && noGamesElement.textContent.includes('No games found')) {
                console.log('Found "No games found" message, showing empty state');
                this.hidePagination();
                return;
            }
        }

        // Обновляем элементы DOM с полученными значениями
        this.updatePageInfoElements();

        console.log(`Pagination info loaded: ${this.state.totalItems} items, ${this.state.totalPages} pages, items per page: ${this.config.itemsPerPage}`);

        this.showPagination();
    },

    // Обновить элементы информации о странице
    updatePageInfoElements() {
        // Обновляем верхние элементы
        const topElements = {
            start: document.querySelector(this.config.startElementTopId),
            end: document.querySelector(this.config.endElementTopId),
            current: document.querySelector(this.config.currentElementTopId),
            total: document.querySelector(this.config.totalElementTopId),
            totalPages: document.querySelector(this.config.totalPagesElementTopId)
        };

        // Обновляем нижние элементы
        const bottomElements = {
            start: document.querySelector(this.config.startElementBottomId),
            end: document.querySelector(this.config.endElementBottomId),
            current: document.querySelector(this.config.currentElementBottomId),
            total: document.querySelector(this.config.totalElementBottomId),
            totalPages: document.querySelector(this.config.totalPagesElementBottomId)
        };

        const updateElementSet = (elements, currentPage) => {
            const startIndex = (currentPage - 1) * this.config.itemsPerPage + 1;
            const endIndex = Math.min(currentPage * this.config.itemsPerPage, this.state.totalItems);

            if (elements.start) elements.start.textContent = startIndex;
            if (elements.end) elements.end.textContent = endIndex;
            if (elements.current) elements.current.textContent = currentPage;
            if (elements.total) elements.total.textContent = this.state.totalItems;
            if (elements.totalPages) elements.totalPages.textContent = this.state.totalPages;
        };

        updateElementSet(topElements, this.config.currentPage);
        updateElementSet(bottomElements, this.config.currentPage);
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

                // Если ошибка "No games found", все равно обрабатываем как пустую страницу
                if (error.message.includes('No games found') ||
                    error.message.includes('Empty response')) {
                    console.log(`Treating error as empty page for page ${pageNumber}`);
                    this.processPageGames(pageNumber, []);
                    this.updatePageButtonState(pageNumber, 'loaded');

                    if (this.state.loadingPages) {
                        this.state.loadingPages.delete(pageNumber);
                    }

                    return [];
                }

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
            console.log(`Loading page ${pageNumber} from server...`);

            // Используем тот же URL что и для основной страницы
            const url = new URL(window.location.href);

            // Удаляем старые параметры пагинации
            url.searchParams.delete('page');
            url.searchParams.delete('_ajax');
            url.searchParams.delete('_scroll');

            // Добавляем новые параметры
            url.searchParams.set('page', pageNumber);
            url.searchParams.set('_ajax', '1');
            url.searchParams.set('_force', '1');

            console.log(`Fetching from: ${url.toString()}`);

            fetch(url.toString())
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    return response.text();
                })
                .then(html => {
                    console.log(`Received ${html.length} chars for page ${pageNumber}`);

                    if (!html || html.trim().length === 0) {
                        reject(new Error('Empty response from server'));
                        return;
                    }

                    const parser = new DOMParser();
                    const doc = parser.parseFromString(html, 'text/html');

                    // ВАЖНО: Ищем именно grid с играми, а не общий контейнер
                    const gamesGrid = doc.querySelector('#games-grid-row');
                    const gamesContainer = gamesGrid || doc.querySelector('.games-container .row');

                    if (!gamesContainer) {
                        console.warn('Games container not found in AJAX response');

                        // Проверяем, есть ли сообщение "No games found"
                        const noGamesElement = doc.querySelector('.text-center.py-5');
                        if (noGamesElement && noGamesElement.textContent.includes('No games found')) {
                            console.log('AJAX returned "No games found"');
                            resolve([]);
                            return;
                        }

                        reject(new Error('Games container not found in AJAX response'));
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

                        console.log(`AJAX returned ${games.length} games for page ${pageNumber}`);
                        resolve(games);
                    } else {
                        // Проверяем, не пустой ли это ответ
                        const containerHTML = gamesContainer.innerHTML.trim();
                        if (containerHTML === '' ||
                            containerHTML.includes('No games found') ||
                            containerHTML.includes('text-center py-5')) {
                            console.log('AJAX returned empty games container');
                            resolve([]);
                        } else {
                            reject(new Error('No games found in AJAX response'));
                        }
                    }
                })
                .catch(error => {
                    console.error(`Error loading page ${pageNumber}:`, error);
                    reject(error);
                });
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

                // ВАЖНО: Убедимся, что это элемент с правильным классом
                if (!gameElement.classList.contains('game-card-container')) {
                    gameElement.classList.add('game-card-container');
                }

                // Сохраняем в state
                this.state.gameItems[gameIndex] = gameElement;

                console.log(`Processed game at index ${gameIndex} for page ${pageNumber}`);
            }
        });

        // ВАЖНО: Добавляем страницу в loadedPages только если есть игры или это сознательно пустая страница
        if (games.length > 0 || pageNumber === 1) {
            this.state.loadedPages.add(pageNumber);
        }

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

        console.log(`showPage called for page ${pageNumber}, isBackground: ${isBackground}`);

        // Если это фоновая загрузка - используем оптимизированный метод
        if (isBackground) {
            this.getOrLoadPageWithCache(pageNumber)
                .then(() => {
                    console.log(`Background loaded page ${pageNumber}`);
                })
                .catch(error => {
                    console.error(`Error background loading page ${pageNumber}:`, error);
                });
            return;
        }

        // Для обычной загрузки
        this.getOrLoadPageWithCache(pageNumber)
            .then(() => {
                console.log(`Page ${pageNumber} loaded successfully, showing...`);
                this.showPageFromCache(pageNumber);
            })
            .catch(error => {
                console.error(`Error loading page ${pageNumber}:`, error);

                // Пробуем показать первую страницу как запасной вариант
                if (pageNumber !== 1 && this.state.loadedPages.has(1)) {
                    console.log(`Falling back to page 1`);
                    this.showPage(1, false);
                }
            });
    },

    // Очистить DOM контейнер ТОЛЬКО при необходимости
    clearDOMContainer(force = false) {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return;

        // ВАЖНО: Всегда полностью очищаем контейнер при переключении страниц
        if (force) {
            rowElement.innerHTML = '';
            console.log('Force cleared DOM container');
            return;
        }

        // Проверяем, нужно ли очищать
        const existingGames = rowElement.querySelectorAll('.game-card-container');
        if (existingGames.length === 0) {
            console.log('No games to clear');
            return;
        }

        // Очищаем ВСЕ игры при переключении страниц
        rowElement.innerHTML = '';
        console.log(`Cleared ${existingGames.length} games from DOM container`);
    },

    // Кешировать игры из DOM
    cacheGamesFromDOM(pageNumber) {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return;

        const gameElements = rowElement.querySelectorAll('.game-card-container');

        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;

        console.log(`Caching ${gameElements.length} games from DOM for page ${pageNumber}, start index: ${startIndex}`);

        gameElements.forEach((element, index) => {
            const gameIndex = startIndex + index;

            // Убедимся, что элемент имеет правильные атрибуты
            element.dataset.page = pageNumber;
            element.dataset.index = gameIndex;

            // Клонируем элемент для кеша
            const clonedElement = element.cloneNode(true);
            clonedElement.style.display = 'none'; // Скрываем в кеше

            this.state.gameItems[gameIndex] = clonedElement;
        });

        console.log(`Total cached games after page ${pageNumber}: ${this.state.gameItems.filter(g => g).length}`);
    },

    // Обновить URL браузера
    updateBrowserUrl(pageNumber) {
        const currentUrl = new URL(window.location.href);
        const currentPageFromUrl = currentUrl.searchParams.get('page');

        console.log(`Updating URL: current page in URL = ${currentPageFromUrl}, new page = ${pageNumber}`);

        // Если мы уже на нужной странице в URL, не меняем URL
        if (currentPageFromUrl && parseInt(currentPageFromUrl) === pageNumber && pageNumber !== 1) {
            console.log(`URL already has page=${pageNumber}, skipping update`);
            return;
        }

        // Если страница = 1 и в URL нет параметра page, тоже не меняем URL
        if (pageNumber === 1 && !currentPageFromUrl) {
            console.log(`Page is 1 and URL has no page param, skipping update`);
            return;
        }

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

        // Сохраняем текущий URL
        const currentUrl = window.location.href;
        const urlParams = new URLSearchParams(window.location.search);
        const currentPageInUrl = urlParams.get('page');

        console.log(`Current URL: ${currentUrl}, page in URL: ${currentPageInUrl}`);

        this.loadPaginationInfoFromDOM();

        this.showPagination();

        if (!document.querySelector(this.config.pageNumbersContainerTopId)) {
            this.createPageNumbersContainer('top');
        }
        if (!document.querySelector(this.config.pageNumbersContainerBottomId)) {
            this.createPageNumbersContainer('bottom');
        }

        this.setupPagination();

        // Восстанавливаем страницу из URL (если есть) или показываем первую
        const pageFromUrl = currentPageInUrl ? parseInt(currentPageInUrl) : 1;
        const initialPage = pageFromUrl >= 1 ? pageFromUrl : 1;

        console.log(`Showing page from URL: ${initialPage}`);

        // Проверяем, загружена ли страница
        if (this.state.loadedPages.has(initialPage)) {
            console.log(`Page ${initialPage} already loaded, showing from cache`);
            this.showPageFromCache(initialPage);
        } else {
            console.log(`Loading page ${initialPage}`);
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

        console.log(`Force update: ${this.state.totalItems} items, ${this.state.totalPages} pages`);

        this.showPagination();

        this.createPageNumbersContainer('top');
        this.createPageNumbersContainer('bottom');

        this.setupPagination();

        // Восстанавливаем страницу из URL
        const initialPage = this.getPageFromURL();

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
                GamePagination.preloadAdjacentPages(GamePagination.getCurrentPage());
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