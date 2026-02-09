// games/static/games/js/game_list/game-pagination-core.js
const GamePaginationCore = {
    config: {
        itemsPerPage: 16,
        currentPage: 1,
        itemsSelector: '.game-card-container',
        containerSelector: '.games-container',
        maxVisiblePages: 7
    },

    state: {
        totalItems: 0,
        totalPages: 0,
        loadedPages: new Set(),
        isLoading: false,
        gameItems: [],
        loadingPages: new Set(),
    },

    init() {
        console.log('=== LAZY GAMES PAGINATION INITIALIZATION OPTIMIZED ===');

        this.state.gameItems = [];
        this.state.loadedPages.clear();

        if (!this.state.loadingPages) {
            this.state.loadingPages = new Set();
        }

        this.loadPaginationInfoFromDOM();

        const noGamesElement = document.querySelector('.text-center.py-5');
        if (noGamesElement && noGamesElement.textContent.includes('No games found')) {
            console.log('Found "No games found" message, hiding pagination');
            this.hidePagination();
            return;
        }

        const serverPageField = document.getElementById('server-current-page');
        let initialPage = 1;

        if (serverPageField) {
            const serverPage = parseInt(serverPageField.value);
            if (!isNaN(serverPage) && serverPage >= 1 && serverPage <= this.state.totalPages) {
                initialPage = serverPage;
                console.log(`Using server page from hidden field: ${initialPage}`);
            } else {
                console.log(`Server page ${serverPage} invalid or out of range (1-${this.state.totalPages})`);
            }
        }

        if (initialPage === 1) {
            const urlPage = this.getPageFromURL();
            console.log(`URL requests page: ${urlPage}`);
            initialPage = urlPage;
        }

        this.config.currentPage = initialPage;

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
            console.log(`Page ${this.config.currentPage} already has ${existingGames.length} games in DOM, skipping reload`);

            this.cacheGamesFromDOM(this.config.currentPage);
            this.state.loadedPages.add(this.config.currentPage);

            if (window.GamePaginationUI && typeof window.GamePaginationUI.init === 'function') {
                window.GamePaginationUI.init();
            }

            this.updatePageInfo();
            this.updateNavigationButtons();

            this.saveFirstPageToLocalCache();

            this.preloadAdjacentPages(this.config.currentPage);

            return;
        }

        console.log(`No correct games in DOM for page ${this.config.currentPage}, loading...`);

        if (window.GamePaginationUI && typeof window.GamePaginationUI.init === 'function') {
            window.GamePaginationUI.init();
        }

        if (this.config.currentPage === 1) {
            const cachedFirstPage = this.loadFirstPageFromLocalCache();
            if (cachedFirstPage && cachedFirstPage.length > 0) {
                console.log(`Loaded page 1 from browser cache: ${cachedFirstPage.length} games`);
                this.processPageGames(1, cachedFirstPage);
                this.showPageFromCache(1);
                return;
            }
        }

        this.checkAndCacheInitialPage();
    },

    getOrLoadPageWithCache(pageNumber) {
        if (pageNumber < 1 || pageNumber > this.state.totalPages) {
            console.log(`Page ${pageNumber} out of range (1-${this.state.totalPages})`);
            return Promise.reject(new Error('Page out of range'));
        }

        console.log(`getOrLoadPageWithCache for page ${pageNumber}`);

        if (this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loaded in memory`);
            return Promise.resolve(this._getPageFromMemory(pageNumber));
        }

        if (pageNumber === 1) {
            const cachedFirstPage = this.loadFirstPageFromLocalCache();
            if (cachedFirstPage && cachedFirstPage.length > 0) {
                console.log(`Loading page 1 from browser cache`);
                this.processPageGames(1, cachedFirstPage);
                return Promise.resolve(cachedFirstPage);
            }
        }

        const sessionCacheKey = `page_${pageNumber}_cached`;
        try {
            const cachedData = sessionStorage.getItem(sessionCacheKey);
            if (cachedData) {
                const data = JSON.parse(cachedData);

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

        console.log(`Loading page ${pageNumber} from server...`);
        return this.forceLoadPage(pageNumber)
            .then(games => {
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

    loadFirstPageFromLocalCache() {
        try {
            const cachedData = localStorage.getItem('gamepagination_firstpage_cache');
            if (!cachedData) return null;

            const data = JSON.parse(cachedData);

            const maxAge = 30 * 60 * 1000;
            if (Date.now() - data.timestamp > maxAge) {
                localStorage.removeItem('gamepagination_firstpage_cache');
                return null;
            }

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

    arePageGamesInDOM(pageNumber) {
        if (!pageNumber || pageNumber < 1) return false;

        const container = document.querySelector(this.config.containerSelector);
        if (!container) return false;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return false;

        const gameElements = rowElement.querySelectorAll('.game-card-container');
        const gameCount = gameElements.length;

        if (gameCount === 0) {
            console.log(`No games in DOM for page check`);
            return false;
        }

        const serverPage = parseInt(container.dataset.serverPage);
        if (!isNaN(serverPage) && serverPage === pageNumber) {
            console.log(`Container has data-server-page="${serverPage}" matching requested page ${pageNumber}`);
            return true;
        }

        let allGamesMatch = true;
        let firstGamePage = null;

        for (const gameElement of gameElements) {
            const gamePage = parseInt(gameElement.dataset.page);

            if (isNaN(gamePage)) {
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

        if (allGamesMatch && firstGamePage === pageNumber) {
            console.log(`All ${gameCount} games in DOM have correct data-page="${pageNumber}"`);
            return true;
        }

        const serverPageField = document.getElementById('server-current-page');
        if (serverPageField) {
            const serverPageValue = parseInt(serverPageField.value);
            if (!isNaN(serverPageValue) && serverPageValue === pageNumber) {
                console.log(`Server field indicates page ${pageNumber}, accepting DOM as correct`);
                return true;
            }
        }

        const expectedCount = (pageNumber === this.state.totalPages)
            ? (this.state.totalItems % this.config.itemsPerPage || this.config.itemsPerPage)
            : this.config.itemsPerPage;

        if (gameCount === expectedCount || gameCount > 0) {
            console.log(`Found ${gameCount} games in DOM, likely for page ${pageNumber}`);
            return true;
        }

        return false;
    },

    checkAndCacheInitialPage() {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return;

        const existingGames = rowElement.querySelectorAll('.game-card-container');
        console.log(`Found ${existingGames.length} games in DOM on init`);

        const loadingMsg = rowElement.querySelector('.games-loading-indicator');
        if (loadingMsg) {
            loadingMsg.remove();
            console.log('Loading indicator removed');
        }

        if (existingGames.length === 0) {
            console.log(`No games in DOM, loading page ${this.config.currentPage}`);
            this.showPage(this.config.currentPage, false);
            return;
        }

        const serverPageField = document.getElementById('server-current-page');
        let pageToCache = this.config.currentPage;

        if (serverPageField) {
            const serverPage = parseInt(serverPageField.value);
            if (!isNaN(serverPage) && serverPage >= 1 && serverPage <= this.state.totalPages) {
                console.log(`Server indicates page ${serverPage}, using it instead of URL page ${this.config.currentPage}`);
                pageToCache = serverPage;
                this.config.currentPage = serverPage;
            }
        }

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

            if (detectedPage !== pageToCache) {
                console.log(`DOM has page ${detectedPage}, but using server/page ${pageToCache}.`);
            }
        }

        this.cacheGamesFromDOM(pageToCache);
        this.state.loadedPages.add(pageToCache);

        console.log(`Cached ${existingGames.length} games from DOM for page ${pageToCache}`);

        if (pageToCache === 1) {
            this.saveFirstPageToLocalCache();
        }

        if (window.GamePaginationUI) {
            window.GamePaginationUI.updatePageInfo();
            window.GamePaginationUI.updateNavigationButtons();
            window.GamePaginationUI.showPagination();
        }

        console.log(`Initial page ${pageToCache} loaded from DOM`);
    },

    getPageFromURL() {
        const urlParams = new URLSearchParams(window.location.search);
        const pageFromURL = urlParams.get('page');

        if (pageFromURL) {
            const parsedPage = parseInt(pageFromURL);
            if (!isNaN(parsedPage) && parsedPage >= 1) {
                if (this.state.totalPages > 0 && parsedPage > this.state.totalPages) {
                    console.log(`Page ${parsedPage} exceeds total pages (${this.state.totalPages}), adjusting to ${this.state.totalPages}`);
                    return this.state.totalPages;
                }
                return parsedPage;
            }
        }

        console.log(`No page in URL, defaulting to page 1`);
        return 1;
    },

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

    restorePageFromStorage() {
        try {
            const saved = sessionStorage.getItem('gamePaginationState');
            if (saved) {
                const state = JSON.parse(saved);

                const age = Date.now() - state.timestamp;
                const maxAge = 5 * 60 * 1000;

                if (age < maxAge && state.url === window.location.href) {
                    console.log(`Restored page ${state.page} from storage for URL: ${state.url}`);
                    return state.page;
                } else {
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

    clearPageState() {
        try {
            sessionStorage.removeItem('gamePaginationState');
            console.log('Cleared page state from storage');
        } catch (e) {
            console.warn('Could not clear page state:', e);
        }
    },

    loadPaginationInfoFromDOM() {
        console.log('Loading pagination info from DOM...');

        const serverTotalElement = document.getElementById('server-total-count');
        const serverTotalPagesElement = document.getElementById('server-total-pages');
        const serverItemsPerPageElement = document.getElementById('server-items-per-page');

        if (serverTotalElement && serverTotalPagesElement) {
            this.state.totalItems = parseInt(serverTotalElement.value) || 0;
            this.state.totalPages = parseInt(serverTotalPagesElement.value) || 1;

            if (serverItemsPerPageElement) {
                this.config.itemsPerPage = parseInt(serverItemsPerPageElement.value) || 16;
            }

            console.log(`Got pagination info from server fields: ${this.state.totalItems} items, ${this.state.totalPages} pages, ${this.config.itemsPerPage} items per page`);
        } else {
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

        if (this.state.totalItems === 0) {
            const totalElement = document.querySelector('#games-total-all');
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
            const totalPagesElement = document.querySelector('#games-total-pages');
            if (totalPagesElement) {
                const totalPagesText = totalPagesElement.textContent.trim();
                const match = totalPagesText.match(/\d+/);
                if (match) {
                    this.state.totalPages = parseInt(match[0]) || 1;
                    console.log(`Got totalPages from visible element: ${this.state.totalPages}`);
                }
            }
        }

        if (this.state.totalItems > 0 && this.state.totalPages <= 1) {
            this.state.totalPages = Math.ceil(this.state.totalItems / this.config.itemsPerPage);
            console.log(`Calculated totalPages: ${this.state.totalPages} from ${this.state.totalItems} items with ${this.config.itemsPerPage} per page`);
        }

        if (this.state.totalItems === 0) {
            console.log('No items found for pagination');
        }

        this.updatePageInfoElements();

        console.log(`Pagination info loaded: ${this.state.totalItems} items, ${this.state.totalPages} pages, items per page: ${this.config.itemsPerPage}`);
    },

    updatePageInfoElements() {
        if (window.GamePaginationUI && typeof window.GamePaginationUI.updatePageInfoElements === 'function') {
            window.GamePaginationUI.updatePageInfoElements(this.config.currentPage, this.state.totalItems, this.state.totalPages, this.config.itemsPerPage);
        }
    },

    shouldUpdateUrlOnPageChange(pageNumber) {
        const urlParams = new URLSearchParams(window.location.search);
        const currentPageInUrl = urlParams.get('page');

        if (pageNumber === 1 && !currentPageInUrl) {
            return false;
        }

        if (currentPageInUrl && parseInt(currentPageInUrl) === pageNumber) {
            return false;
        }

        return true;
    },

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

        const loadingMsg = rowElement.querySelector('.games-loading-indicator');
        if (loadingMsg) {
            loadingMsg.remove();
        }

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

            rowElement.innerHTML = '';

            if (gamesAdded === 0 && pageNumber === 1) {
                const noGamesTemplate = document.getElementById('no-games-template');
                if (noGamesTemplate) {
                    const templateClone = noGamesTemplate.cloneNode(true);
                    templateClone.style.display = 'block';
                    rowElement.appendChild(templateClone);
                    console.log('Showed "No games found" message');
                    if (window.GamePaginationUI && typeof window.GamePaginationUI.hidePagination === 'function') {
                        window.GamePaginationUI.hidePagination();
                    }
                }
            } else {
                rowElement.appendChild(fragment);
                console.log(`Added ${gamesAdded} games from cache to page ${pageNumber}`);
                if (window.GamePaginationUI && typeof window.GamePaginationUI.showPagination === 'function') {
                    window.GamePaginationUI.showPagination();
                }
            }
        } else {
            console.log(`DOM already has correct games for page ${pageNumber}, skipping update`);
        }

        if (window.GamePaginationUI) {
            window.GamePaginationUI.updatePageNumbers();
            window.GamePaginationUI.updatePageInfo();
            window.GamePaginationUI.updateNavigationButtons();
        }

        if (this.shouldUpdateUrlOnPageChange(pageNumber)) {
            this.updateBrowserUrl(pageNumber);
        }

        this.saveCurrentPageToStorage(pageNumber);

        this.preloadAdjacentPages(pageNumber);
    },

    clearAllCache() {
        console.log('Clearing all pagination cache...');

        this.state.gameItems = [];
        this.state.loadedPages.clear();

        if (this.state.loadingPages) {
            this.state.loadingPages.clear();
        }

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

    clearAllGameElements() {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (rowElement) {
            const hiddenElements = rowElement.querySelectorAll('input[type="hidden"], #server-debug-info, #extended-debug-info');
            const hiddenElementsHTML = Array.from(hiddenElements).map(el => el.outerHTML).join('');

            rowElement.innerHTML = hiddenElementsHTML;
            console.log('Cleared all game elements from DOM, kept hidden fields');
        }
    },

    showLoadingIndicator(pageNumber) {
        const existingIndicator = document.getElementById(`loading-page-${pageNumber}`);
        if (existingIndicator) {
            console.log(`Loading indicator for page ${pageNumber} already exists`);
            return;
        }

        if (this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loaded, skipping indicator`);
            return;
        }

        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        console.log(`Creating loading indicator for page ${pageNumber}`);

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

        let rowElement = container.querySelector('.row');
        if (!rowElement) {
            rowElement = document.createElement('div');
            rowElement.className = 'row';
            container.appendChild(rowElement);
        }

        loadingDiv.style.position = 'absolute';
        loadingDiv.style.zIndex = '1000';

        rowElement.style.position = 'relative';

        rowElement.appendChild(loadingDiv);
    },

    removeLoadingIndicator(pageNumber) {
        const loadingIndicator = document.getElementById(`loading-page-${pageNumber}`);
        if (loadingIndicator) {
            console.log(`Removing loading indicator for page ${pageNumber}`);
            loadingIndicator.remove();
            return true;
        }

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

    loadPageFromServer(pageNumber) {
        return new Promise((resolve, reject) => {
            console.log(`Loading page ${pageNumber} from server...`);

            const url = new URL(window.location.href);

            url.searchParams.delete('page');
            url.searchParams.delete('_ajax');
            url.searchParams.delete('_scroll');

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

                    const gamesGrid = doc.querySelector('#games-grid-row');
                    const gamesContainer = gamesGrid || doc.querySelector('.games-container .row');

                    if (!gamesContainer) {
                        console.warn('Games container not found in AJAX response');

                        const noGamesElement = doc.querySelector('.text-center.py-5');
                        if (noGamesElement && noGamesElement.textContent.includes('No games found')) {
                            console.log('AJAX returned "No games found"');
                            resolve([]);
                            return;
                        }

                        reject(new Error('Games container not found in AJAX response'));
                        return;
                    }

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

    processPageGames(pageNumber, games) {
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;

        console.log(`Processing page ${pageNumber}, start index: ${startIndex}, games to process: ${games.length}`);

        if (!Array.isArray(this.state.gameItems)) {
            this.state.gameItems = [];
        }

        games.forEach((gameHtml, index) => {
            const gameIndex = startIndex + index;

            const tempDiv = document.createElement('div');
            tempDiv.innerHTML = gameHtml;
            const gameElement = tempDiv.firstChild;

            if (gameElement) {
                gameElement.dataset.page = pageNumber;
                gameElement.dataset.index = gameIndex;
                gameElement.style.display = 'none';

                if (!gameElement.classList.contains('game-card-container')) {
                    gameElement.classList.add('game-card-container');
                }

                this.state.gameItems[gameIndex] = gameElement;

                console.log(`Processed game at index ${gameIndex} for page ${pageNumber}`);
            }
        });

        if (games.length > 0 || pageNumber === 1) {
            this.state.loadedPages.add(pageNumber);
        }

        if (window.GamePaginationUI && typeof window.GamePaginationUI.updatePageButtonState === 'function') {
            window.GamePaginationUI.updatePageButtonState(pageNumber, 'loaded');
        }

        console.log(`Page ${pageNumber} processed (${games.length} games), total loaded pages: ${this.state.loadedPages.size}`);
    },

    savePageToStorage(pageNumber, games) {
        try {
            const key = `page_${pageNumber}_games`;
            sessionStorage.setItem(key, JSON.stringify(games));
        } catch (error) {
            console.error(`Error saving page ${pageNumber} to storage:`, error);
        }
    },

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

    updateBrowserUrl(pageNumber) {
        const currentUrl = new URL(window.location.href);
        const currentPageFromUrl = currentUrl.searchParams.get('page');

        console.log(`Updating URL: current page in URL = ${currentPageFromUrl}, new page = ${pageNumber}`);

        if (currentPageFromUrl && parseInt(currentPageFromUrl) === pageNumber && pageNumber !== 1) {
            console.log(`URL already has page=${pageNumber}, skipping update`);
            return;
        }

        if (pageNumber === 1 && !currentPageFromUrl) {
            console.log(`Page is 1 and URL has no page param, skipping update`);
            return;
        }

        const url = new URL(window.location.href);

        if (pageNumber === 1) {
            url.searchParams.delete('page');
        } else {
            url.searchParams.set('page', pageNumber);
        }

        window.history.replaceState({
            page: pageNumber,
            timestamp: Date.now()
        }, document.title, url.toString());

        console.log(`Updated URL to: ${url.toString()}`);
    },

    preloadAdjacentPages(currentPage) {
        const pagesToPreload = [];

        for (let page = 1; page <= this.state.totalPages; page++) {
            if (page === currentPage) {
                continue;
            }

            if (this.state.loadedPages.has(page)) {
                continue;
            }

            if (this.state.loadingPages && this.state.loadingPages.has(page)) {
                continue;
            }

            pagesToPreload.push(page);
        }

        console.log(`Preloading ${pagesToPreload.length} pages in background...`);

        pagesToPreload.forEach((page, index) => {
            setTimeout(() => {
                console.log(`Background loading page ${page}...`);
                this.loadPageInBackground(page);
            }, index * 500);
        });
    },

    loadPageInBackground(pageNumber) {
        if (this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loaded, skipping background load`);
            return;
        }

        if (this.state.loadingPages && this.state.loadingPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loading, skipping duplicate`);
            return;
        }

        const cachedGames = this.getPageFromStorage(pageNumber);
        if (cachedGames && cachedGames.length > 0) {
            console.log(`Found page ${pageNumber} in cache, processing...`);
            this.processPageGames(pageNumber, cachedGames);
            return;
        }

        console.log(`Background loading page ${pageNumber}...`);

        this.loadPageInBackgroundDirect(pageNumber);
    },

    loadPageInBackgroundDirect(pageNumber) {
        if (!this.state.loadingPages) {
            this.state.loadingPages = new Set();
        }
        this.state.loadingPages.add(pageNumber);

        this.loadPageFromServer(pageNumber)
            .then(games => {
                this.processPageGames(pageNumber, games);
                console.log(`Page ${pageNumber} loaded in background (${games.length} games)`);

                if (this.state.loadingPages) {
                    this.state.loadingPages.delete(pageNumber);
                }
            })
            .catch(error => {
                console.error(`Error loading page ${pageNumber} in background:`, error);

                this.processPageGames(pageNumber, []);

                if (this.state.loadingPages) {
                    this.state.loadingPages.delete(pageNumber);
                }
            });
    },

    updatePageInfo() {
        const startIndex = (this.config.currentPage - 1) * this.config.itemsPerPage + 1;
        const endIndex = Math.min(this.config.currentPage * this.config.itemsPerPage, this.state.totalItems);

        if (window.GamePaginationUI && typeof window.GamePaginationUI.updateSinglePageInfo === 'function') {
            window.GamePaginationUI.updateSinglePageInfo('top', startIndex, endIndex);
            window.GamePaginationUI.updateSinglePageInfo('bottom', startIndex, endIndex);
        }
    },

    updateSinglePageInfo(position, startIndex, endIndex) {
        if (window.GamePaginationUI && typeof window.GamePaginationUI.updateSinglePageInfo === 'function') {
            window.GamePaginationUI.updateSinglePageInfo(position, startIndex, endIndex);
        }
    },

    updateNavigationButtons() {
        if (window.GamePaginationUI && typeof window.GamePaginationUI.updateNavigationButtonPair === 'function') {
            window.GamePaginationUI.updateNavigationButtonPair('top');
            window.GamePaginationUI.updateNavigationButtonPair('bottom');
        }
    },

    updateNavigationButtonPair(position) {
        if (window.GamePaginationUI && typeof window.GamePaginationUI.updateNavigationButtonPair === 'function') {
            window.GamePaginationUI.updateNavigationButtonPair(position);
        }
    },

    updatePageButtonState(pageNumber, state) {
        if (window.GamePaginationUI && typeof window.GamePaginationUI.updatePageButtonState === 'function') {
            window.GamePaginationUI.updatePageButtonState(pageNumber, state);
        }
    },

    hidePagination() {
        if (window.GamePaginationUI && typeof window.GamePaginationUI.hidePagination === 'function') {
            window.GamePaginationUI.hidePagination();
        }
    },

    showPagination() {
        if (window.GamePaginationUI && typeof window.GamePaginationUI.showPagination === 'function') {
            window.GamePaginationUI.showPagination();
        }
    },

    updateAfterChanges() {
        console.log('Updating lazy pagination after changes...');

        const currentUrl = window.location.href;
        const urlParams = new URLSearchParams(window.location.search);
        const currentPageInUrl = urlParams.get('page');

        console.log(`Current URL: ${currentUrl}, page in URL: ${currentPageInUrl}`);

        this.loadPaginationInfoFromDOM();

        const noGamesElement = document.querySelector('.text-center.py-5');
        if (noGamesElement && noGamesElement.textContent.includes('No games found')) {
            console.log('Found "No games found" message, hiding pagination');
            this.hidePagination();
            return;
        }

        this.showPagination();

        if (window.GamePaginationUI && typeof window.GamePaginationUI.init === 'function') {
            window.GamePaginationUI.init();
        }

        const pageFromUrl = currentPageInUrl ? parseInt(currentPageInUrl) : 1;
        const initialPage = pageFromUrl >= 1 ? pageFromUrl : 1;

        console.log(`Showing page from URL: ${initialPage}`);

        if (this.state.loadedPages.has(initialPage)) {
            console.log(`Page ${initialPage} already loaded, showing from cache`);
            this.showPageFromCache(initialPage);
        } else {
            console.log(`Loading page ${initialPage}`);
            this.showPage(initialPage, false);
        }
    },

    resetToFirstPage() {
        this.clearPageState();
        this.showPage(1, false);
    },

    forceUpdate() {
        console.log('Force updating lazy pagination...');

        this.loadPaginationInfoFromDOM();

        console.log(`Force update: ${this.state.totalItems} items, ${this.state.totalPages} pages`);

        this.showPagination();

        if (window.GamePaginationUI && typeof window.GamePaginationUI.init === 'function') {
            window.GamePaginationUI.init();
        }

        const initialPage = this.getPageFromURL();

        if (!this.state.loadedPages.has(initialPage)) {
            if (initialPage > 1) {
                console.log(`Force update: loading page ${initialPage}`);
                this.forceLoadPage(initialPage).then(() => {
                    this.showPage(initialPage, false);
                }).catch(() => {
                    this.showPage(initialPage, false);
                });
            } else {
                this.showPage(initialPage, false);
            }
        } else {
            this.showPage(initialPage, false);
        }
    },

    getCurrentPage() {
        return this.config.currentPage;
    },

    getTotalPages() {
        return this.state.totalPages;
    },

    hasPagination() {
        return this.state.totalPages > 1;
    },

    destroy() {
        console.log('Destroying lazy game pagination...');
        this.state.gameItems = [];
        this.state.loadedPages.clear();
        this.clearPageState();
    },

    showPage(pageNumber, isBackground = false) {
        if (pageNumber < 1 || pageNumber > this.state.totalPages) {
            console.log(`Page ${pageNumber} out of range (1-${this.state.totalPages})`);
            return;
        }

        console.log(`showPage called for page ${pageNumber}, isBackground: ${isBackground}`);

        if (!isBackground) {
            // Убрали создание темного экрана загрузки
            console.log(`Loading page ${pageNumber}...`);

            // Просто добавляем легкий класс loading к контейнеру (без затемнения)
            const container = document.querySelector(this.config.containerSelector);
            if (container) {
                container.classList.add('loading');
            }
        }

        if (this.state.loadedPages.has(pageNumber)) {
            console.log(`Page ${pageNumber} already loaded, showing...`);
            this.showPageFromCache(pageNumber);

            if (!isBackground) {
                // Убираем класс loading
                const container = document.querySelector(this.config.containerSelector);
                if (container) {
                    container.classList.remove('loading');
                }
            }
            return;
        }

        this.getOrLoadPageWithCache(pageNumber)
            .then(() => {
                console.log(`Page ${pageNumber} loaded successfully, showing...`);
                this.showPageFromCache(pageNumber);

                if (!isBackground) {
                    // Убираем класс loading после загрузки
                    const container = document.querySelector(this.config.containerSelector);
                    if (container) {
                        container.classList.remove('loading');
                    }
                }
            })
            .catch(error => {
                console.error(`Error loading page ${pageNumber}:`, error);

                if (pageNumber !== 1 && this.state.loadedPages.has(1)) {
                    console.log(`Falling back to page 1`);
                    this.showPageFromCache(1);
                }

                if (!isBackground) {
                    // Убираем класс loading при ошибке
                    const container = document.querySelector(this.config.containerSelector);
                    if (container) {
                        container.classList.remove('loading');
                    }
                }
            });
    },

    showSimpleLoadingIndicator(pageNumber) {
        const container = document.querySelector(this.config.containerSelector);
        if (container) {
            container.classList.add('loading');
        }
    },

    removeSimpleLoadingIndicator() {
        const container = document.querySelector(this.config.containerSelector);
        if (container) {
            container.classList.remove('loading');
        }
    },

    forceLoadPage(pageNumber) {
        if (window.GamePaginationUI && typeof window.GamePaginationUI.updatePageButtonState === 'function') {
            window.GamePaginationUI.updatePageButtonState(pageNumber, 'loading');
        }

        const isCurrentPage = this.config.currentPage === pageNumber;
        if (isCurrentPage) {
            // Убираем создание темного экрана, только добавляем класс loading
            const container = document.querySelector(this.config.containerSelector);
            if (container) {
                container.classList.add('loading');
            }
        }

        return this.loadPageFromServer(pageNumber)
            .then(games => {
                this.processPageGames(pageNumber, games);

                if (window.GamePaginationUI && typeof window.GamePaginationUI.updatePageButtonState === 'function') {
                    window.GamePaginationUI.updatePageButtonState(pageNumber, 'loaded');
                }

                if (isCurrentPage) {
                    this.showPageFromCache(pageNumber);
                    // Убираем класс loading
                    const container = document.querySelector(this.config.containerSelector);
                    if (container) {
                        container.classList.remove('loading');
                    }
                }

                return games;
            })
            .catch(error => {
                console.error(`Error force loading page ${pageNumber}:`, error);

                if (window.GamePaginationUI && typeof window.GamePaginationUI.updatePageButtonState === 'function') {
                    window.GamePaginationUI.updatePageButtonState(pageNumber, 'error');
                }

                if (isCurrentPage) {
                    // Убираем класс loading при ошибке
                    const container = document.querySelector(this.config.containerSelector);
                    if (container) {
                        container.classList.remove('loading');
                    }
                }

                throw error;
            });
    },

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

            element.dataset.page = pageNumber;
            element.dataset.index = gameIndex;

            const clonedElement = element.cloneNode(true);
            clonedElement.style.display = 'none';

            this.state.gameItems[gameIndex] = clonedElement;
        });

        console.log(`Total cached games after page ${pageNumber}: ${this.state.gameItems.filter(g => g).length}`);
    },

    clearDOMContainer(force = false) {
        const container = document.querySelector(this.config.containerSelector);
        if (!container) return;

        const rowElement = container.querySelector('.row');
        if (!rowElement) return;

        if (force) {
            rowElement.innerHTML = '';
            console.log('Force cleared DOM container');
            return;
        }

        const existingGames = rowElement.querySelectorAll('.game-card-container');
        if (existingGames.length === 0) {
            console.log('No games to clear');
            return;
        }

        rowElement.innerHTML = '';
        console.log(`Cleared ${existingGames.length} games from DOM container`);
    }
};

if (typeof module !== 'undefined' && module.exports) {
    module.exports = GamePaginationCore;
} else {
    window.GamePaginationCore = GamePaginationCore;
}

export default GamePaginationCore;