// games/static/games/js/game_list/ajax-pagination.js
// AJAX пагинация для списка игр с предзагрузкой и кэшированием соседних страниц

// Добавляем служебный объект для таймеров
const AjaxPaginationDebugTimer = {
    marks: {},
    start(label) {
        this.marks[label] = performance.now();
    },
    end(label) {
        const endTime = performance.now();
        const startTime = this.marks[label];
        if (startTime) {
            const duration = (endTime - startTime).toFixed(2);
            console.warn(`[TIMER] ${label} took ${duration} ms`);
            delete this.marks[label];
        } else {
            console.warn(`[TIMER] No start mark found for: ${label}`);
        }
    }
};

(function() {
    // Флаг, чтобы предотвратить множественную инициализацию
    if (window.ajaxPaginationInitialized) {
        console.log('AjaxPagination: Already initialized');
        return;
    }
    window.ajaxPaginationInitialized = true;

    console.log('AjaxPagination: Initializing');

    // Простой кэш в памяти
    const pageCache = new Map();
    const PAGE_CACHE_EXPIRY = 30 * 60 * 1000; // 30 минут

    // Базовый URL для AJAX запросов
    const AJAX_URL = '/ajax/load-games-page/';

    // Флаг для отслеживания, идет ли сейчас предзагрузка
    let isPrefetching = false;

    // Запускаем сразу после загрузки скрипта
    initialize();

    function initialize() {
        AjaxPaginationDebugTimer.start('AjaxPagination.initialize');
        // Очищаем устаревший кэш
        clearExpiredCache();

        // Перехватываем все клики по ссылкам пагинации
        setupEventListeners();

        // Загружаем текущую страницу через AJAX
        loadInitialPage();
        AjaxPaginationDebugTimer.end('AjaxPagination.initialize');
    }

    function setupEventListeners() {
        // Делегирование событий
        document.addEventListener('click', function(e) {
            const target = e.target.closest('.ajax-pagination-link');
            if (!target) return;

            const parentItem = target.closest('.page-item');
            if (parentItem && parentItem.classList.contains('disabled')) {
                e.preventDefault();
                return;
            }

            e.preventDefault();

            const href = target.getAttribute('href');
            const pageNum = target.dataset.page;

            if (!href) {
                console.error('AjaxPagination: No href found in link');
                return;
            }

            console.log('AjaxPagination: Loading page', pageNum, 'from href:', href);

            // Извлекаем параметры из href
            const params = extractParamsFromUrl(href);

            // Строим URL для AJAX запроса
            const ajaxUrl = buildAjaxUrl(params, pageNum);

            // Строим красивый URL для адресной строки - ВСЕГДА с page параметром
            const displayUrl = buildDisplayUrl(params, pageNum, true);

            console.log('AjaxPagination: AJAX URL:', ajaxUrl);
            console.log('AjaxPagination: Display URL:', displayUrl);

            // Загружаем ТОЛЬКО запрошенную страницу
            loadRequestedPage(ajaxUrl, displayUrl, pageNum);
        });

        // Обработка кнопок назад/вперед
        window.addEventListener('popstate', function(e) {
            if (e.state && e.state.displayUrl) {
                console.log('AjaxPagination: Popstate event');
                const pageNum = getPageFromUrl(e.state.displayUrl);

                // Извлекаем параметры из displayUrl
                const params = extractParamsFromUrl(e.state.displayUrl);

                // Строим URL для AJAX запроса
                const ajaxUrl = buildAjaxUrl(params, pageNum);

                console.log('AjaxPagination: Popstate - AJAX URL:', ajaxUrl);
                console.log('AjaxPagination: Popstate - Display URL:', e.state.displayUrl);

                loadRequestedPage(ajaxUrl, e.state.displayUrl, pageNum, true);
            }
        });
    }

    function loadInitialPage() {
        AjaxPaginationDebugTimer.start('loadInitialPage');
        // Получаем текущий URL
        const currentUrl = window.location.href;

        // Получаем номер страницы из URL
        const pageNum = getPageFromUrl(currentUrl);

        // Проверяем, есть ли параметры похожих игр
        const findSimilar = document.getElementById('server-find-similar')?.value === '1';
        const sourceGameId = document.getElementById('server-source-game-id')?.value;

        console.log('AjaxPagination: Loading initial page via AJAX', {
            page: pageNum,
            findSimilar: findSimilar,
            sourceGameId: sourceGameId
        });

        // Извлекаем параметры из текущего URL
        const params = extractParamsFromUrl(currentUrl);

        // Если есть source_game в скрытых полях, добавляем его
        if (sourceGameId && !params.has('source_game')) {
            params.set('source_game', sourceGameId);
        }

        // Если есть find_similar в скрытых полях, добавляем его
        if (findSimilar && !params.has('find_similar')) {
            params.set('find_similar', '1');
        }

        // Строим URL для AJAX запроса
        const ajaxUrl = buildAjaxUrl(params, pageNum);

        // Строим display URL - ВСЕГДА с page параметром, даже для первой страницы
        const displayUrl = buildDisplayUrl(params, pageNum, true);

        // Показываем загрузку
        showLoading();

        // Загружаем страницу через AJAX
        fetch(ajaxUrl, {
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'text/html'
            },
            credentials: 'same-origin'
        })
        .then(response => {
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            return response.text();
        })
        .then(html => {
            console.log('AjaxPagination: Initial page loaded');

            // Обновляем контейнер
            const gamesResultsContainer = document.getElementById('games-results-container');
            if (gamesResultsContainer) {
                gamesResultsContainer.innerHTML = html;
            }

            // Скрываем загрузку
            hideLoading();

            // Обновляем URL - ВСЕГДА с page параметром
            updateUrlParams(displayUrl, pageNum, true);

            // Переинициализируем компоненты
            reinitializeComponents();

            // Кэшируем текущую страницу
            cacheCurrentPage();

            // Ждем 500ms после полной загрузки, потом начинаем предзагрузку соседних страниц
            setTimeout(() => {
                // Проверяем, что пользователь еще не начал новую загрузку
                if (!isPrefetching) {
                    prefetchAdjacentPages(pageNum, displayUrl);
                }
            }, 500);
        })
        .catch(error => {
            console.error('AjaxPagination: Error loading initial page:', error);
            hideLoading();
        });
        AjaxPaginationDebugTimer.end('loadInitialPage');
    }

    /**
     * Извлекает параметры из URL
     */
    function extractParamsFromUrl(url) {
        try {
            const urlObj = new URL(url, window.location.origin);
            const params = new URLSearchParams(urlObj.search);

            // Удаляем параметр page
            params.delete('page');

            return params;
        } catch (e) {
            console.error('AjaxPagination: Error extracting params from URL', e);
            return new URLSearchParams();
        }
    }

    /**
     * Строит URL для AJAX запроса
     */
    function buildAjaxUrl(params, pageNum) {
        // Копируем параметры
        const ajaxParams = new URLSearchParams(params);

        // Устанавливаем page
        ajaxParams.set('page', pageNum);

        // Строим URL
        return AJAX_URL + '?' + ajaxParams.toString();
    }

    /**
     * Строит красивый URL для адресной строки
     * @param {URLSearchParams} params - параметры без page
     * @param {string} pageNum - номер страницы
     * @param {boolean} alwaysIncludePage - всегда включать page даже для первой страницы
     */
    function buildDisplayUrl(params, pageNum, alwaysIncludePage = false) {
        // Копируем параметры
        const displayParams = new URLSearchParams(params);

        // Добавляем page если это не первая страница или alwaysIncludePage=true
        if (alwaysIncludePage || pageNum !== '1') {
            displayParams.set('page', pageNum);
        }

        // Строим URL
        const basePath = window.location.pathname;
        const queryString = displayParams.toString();
        return basePath + (queryString ? '?' + queryString : '');
    }

    /**
     * Кэширует текущую страницу
     */
    function cacheCurrentPage() {
        AjaxPaginationDebugTimer.start('cacheCurrentPage');
        const gamesResultsContainer = document.getElementById('games-results-container');
        const currentPageInput = document.getElementById('server-current-page');
        const currentUrl = window.location.href;

        if (!gamesResultsContainer || !currentPageInput) {
            console.log('AjaxPagination: Cannot cache current page - elements not found');
            AjaxPaginationDebugTimer.end('cacheCurrentPage');
            return;
        }

        const currentPage = currentPageInput.value;
        if (!currentPage) {
            console.log('AjaxPagination: Cannot cache current page - page number not found');
            AjaxPaginationDebugTimer.end('cacheCurrentPage');
            return;
        }

        const cacheKey = generateCacheKey(currentUrl, currentPage);
        const currentHtml = gamesResultsContainer.innerHTML;

        if (!getPageFromCache(cacheKey)) {
            savePageToCache(cacheKey, currentHtml, currentPage);
            console.log('AjaxPagination: Cached current page', currentPage, 'with key:', cacheKey);
        }
        AjaxPaginationDebugTimer.end('cacheCurrentPage');
    }

    /**
     * Переинициализирует все компоненты после загрузки нового контента
     */
    function reinitializeComponents() {
        AjaxPaginationDebugTimer.start('reinitializeComponents');
        console.log('AjaxPagination: Reinitializing components');

        // Переинициализируем Bootstrap tooltips
        if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
            const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
            tooltipTriggerList.map(function (tooltipTriggerEl) {
                const tooltip = bootstrap.Tooltip.getInstance(tooltipTriggerEl);
                if (tooltip) {
                    tooltip.dispose();
                }
                return new bootstrap.Tooltip(tooltipTriggerEl);
            });
        }

        // Переинициализируем Bootstrap popovers если есть
        if (typeof bootstrap !== 'undefined' && bootstrap.Popover) {
            const popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
            popoverTriggerList.map(function (popoverTriggerEl) {
                const popover = bootstrap.Popover.getInstance(popoverTriggerEl);
                if (popover) {
                    popover.dispose();
                }
                return new bootstrap.Popover(popoverTriggerEl);
            });
        }

        // Перезапускаем similarity-compare кнопки если они есть
        if (window.SimilarityCompare && typeof window.SimilarityCompare.init === 'function') {
            window.SimilarityCompare.init();
        }

        // Вызываем событие для других модулей
        document.dispatchEvent(new CustomEvent('ajax-content-loaded'));
        AjaxPaginationDebugTimer.end('reinitializeComponents');
    }

    /**
     * Загружает только запрошенную страницу, без предзагрузки
     */
    function loadRequestedPage(ajaxUrl, displayUrl, pageNum, isPopState = false) {
        AjaxPaginationDebugTimer.start('loadRequestedPage');

        // Отменяем любую запланированную предзагрузку
        isPrefetching = false;

        const gamesResultsContainer = document.getElementById('games-results-container');

        if (!gamesResultsContainer) {
            console.error('AjaxPagination: Games results container not found');
            AjaxPaginationDebugTimer.end('loadRequestedPage');
            return;
        }

        const cacheKey = generateCacheKey(displayUrl, pageNum);

        // Проверяем кэш
        const cachedPage = getPageFromCache(cacheKey);
        if (cachedPage) {
            console.log('AjaxPagination: Loading from cache, page', pageNum, 'key:', cacheKey);

            gamesResultsContainer.innerHTML = cachedPage.html;

            if (!isPopState) {
                updateUrlParams(displayUrl, pageNum);
            }

            reinitializeComponents();

            document.dispatchEvent(new CustomEvent('games-grid-updated', {
                detail: { page: pageNum, source: 'cache' }
            }));

            // Планируем предзагрузку через 500ms
            setTimeout(() => {
                if (!isPrefetching) {
                    prefetchAdjacentPages(pageNum, displayUrl);
                }
            }, 500);

            AjaxPaginationDebugTimer.end('loadRequestedPage');
            return;
        }

        console.log('AjaxPagination: Loading from server, page', pageNum);

        showLoading();

        fetch(ajaxUrl, {
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'text/html'
            },
            credentials: 'same-origin'
        })
        .then(response => {
            if (!response.ok) throw new Error(`HTTP error ${response.status}`);
            return response.text();
        })
        .then(html => {
            // Сохраняем в кэш
            savePageToCache(cacheKey, html, pageNum);
            console.log('AjaxPagination: Cached page', pageNum, 'with key:', cacheKey);

            gamesResultsContainer.innerHTML = html;
            hideLoading();

            if (!isPopState) {
                updateUrlParams(displayUrl, pageNum);
            }

            reinitializeComponents();

            document.dispatchEvent(new CustomEvent('games-grid-updated', {
                detail: { page: pageNum, source: 'server' }
            }));

            // Планируем предзагрузку через 500ms после полной загрузки
            setTimeout(() => {
                if (!isPrefetching) {
                    prefetchAdjacentPages(pageNum, displayUrl);
                }
            }, 500);
        })
        .catch(error => {
            console.error('AjaxPagination: Error:', error);
            hideLoading();
            showErrorMessage('Failed to load page. Please refresh.');
        });
        AjaxPaginationDebugTimer.end('loadRequestedPage');
    }

    /**
     * Показывает индикатор загрузки
     */
    function showLoading() {
        const gamesContainer = document.querySelector('.games-container');
        if (gamesContainer) {
            // Добавляем класс loading-visible - CSS покажет спиннер и текст по центру
            gamesContainer.classList.add('loading-visible');

            // Очищаем сетку игр
            const gamesGridRow = document.getElementById('games-grid-row');
            if (gamesGridRow) {
                gamesGridRow.innerHTML = '';
            }
        }

        // Блокируем пагинацию
        document.querySelectorAll('.ajax-pagination-link').forEach(link => {
            link.style.pointerEvents = 'none';
            link.style.opacity = '0.6';
        });
    }

    /**
     * Скрывает индикатор загрузки
     */
    function hideLoading() {
        const gamesContainer = document.querySelector('.games-container');
        if (gamesContainer) {
            // Убираем класс loading-visible - CSS скроет спиннер и текст
            gamesContainer.classList.remove('loading-visible');
        }

        // Разблокируем пагинацию
        document.querySelectorAll('.ajax-pagination-link').forEach(link => {
            link.style.pointerEvents = 'auto';
            link.style.opacity = '1';
        });
    }

    /**
     * Предзагружает соседние страницы в фоне, по одной с малыми задержками
     */
    function prefetchAdjacentPages(currentPage, baseDisplayUrl) {
        AjaxPaginationDebugTimer.start('prefetchAdjacentPages');

        // Устанавливаем флаг, что началась предзагрузка
        isPrefetching = true;

        const totalPagesInput = document.getElementById('server-total-pages');
        if (!totalPagesInput) {
            isPrefetching = false;
            AjaxPaginationDebugTimer.end('prefetchAdjacentPages');
            return;
        }

        const totalPages = parseInt(totalPagesInput.value, 10);
        if (isNaN(totalPages) || totalPages <= 1) {
            isPrefetching = false;
            AjaxPaginationDebugTimer.end('prefetchAdjacentPages');
            return;
        }

        currentPage = parseInt(currentPage, 10);

        // Определяем страницы для предзагрузки: сначала предыдущая, потом следующая
        const pagesToPrefetch = [];

        // Сначала предыдущая страница (если есть)
        if (currentPage - 1 >= 1) {
            pagesToPrefetch.push(currentPage - 1);
        }

        // Потом следующая страница (если есть)
        if (currentPage + 1 <= totalPages) {
            pagesToPrefetch.push(currentPage + 1);
        }

        // Потом дальние
        if (currentPage - 2 >= 1 && !pagesToPrefetch.includes(currentPage - 2)) {
            pagesToPrefetch.push(currentPage - 2);
        }
        if (currentPage + 2 <= totalPages && !pagesToPrefetch.includes(currentPage + 2)) {
            pagesToPrefetch.push(currentPage + 2);
        }

        console.log('AjaxPagination: Will prefetch pages in background with delays:', pagesToPrefetch);

        // Загружаем страницы последовательно с малыми задержками
        pagesToPrefetch.forEach((page, index) => {
            // Задержка: 200ms, 400ms, 600ms, 800ms - быстро, но не блокирует UI
            const delay = 200 + (index * 200);

            setTimeout(() => {
                // Проверяем, не начал ли пользователь новую навигацию
                if (isPrefetching) {
                    prefetchSinglePage(page, baseDisplayUrl);
                }
            }, delay);
        });

        // Сбрасываем флаг через 3 секунды (после завершения всех предзагрузок)
        setTimeout(() => {
            isPrefetching = false;
        }, 3000);

        AjaxPaginationDebugTimer.end('prefetchAdjacentPages');
    }

    /**
     * Предзагружает одну страницу
     */
    function prefetchSinglePage(pageNum, baseDisplayUrl) {
        AjaxPaginationDebugTimer.start('prefetchSinglePage');

        // Проверяем, не начал ли пользователь новую навигацию
        if (!isPrefetching) {
            AjaxPaginationDebugTimer.end('prefetchSinglePage');
            return;
        }

        const params = extractParamsFromUrl(baseDisplayUrl);
        const ajaxUrl = buildAjaxUrl(params, pageNum);
        // Для предзагрузки тоже используем URL с page параметром
        const displayUrl = buildDisplayUrl(params, pageNum, true);
        const cacheKey = generateCacheKey(displayUrl, pageNum);

        if (getPageFromCache(cacheKey)) {
            console.log('AjaxPagination: Page', pageNum, 'already in cache, skipping prefetch');
            AjaxPaginationDebugTimer.end('prefetchSinglePage');
            return;
        }

        console.log('AjaxPagination: Prefetching page', pageNum);

        fetch(ajaxUrl, {
            headers: { 'X-Requested-With': 'XMLHttpRequest' },
            credentials: 'same-origin'
        })
        .then(response => response.ok ? response.text() : null)
        .then(html => {
            if (html && isPrefetching) {
                savePageToCache(cacheKey, html, pageNum);
                console.log('AjaxPagination: Successfully prefetched page', pageNum);
            }
        })
        .catch(() => {});
        AjaxPaginationDebugTimer.end('prefetchSinglePage');
    }

    /**
     * Генерирует ключ кэша
     */
    function generateCacheKey(url, pageNum) {
        try {
            const urlObj = new URL(url, window.location.origin);

            // Для логирования
            const originalParams = {};
            for (const [key, value] of urlObj.searchParams.entries()) {
                originalParams[key] = value;
            }

            // Создаем копию параметров для ключа
            const params = new URLSearchParams(urlObj.search);

            // Удаляем page из параметров для ключа (мы уже используем pageNum отдельно)
            params.delete('page');

            const sortedParams = Array.from(params.entries())
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([k, v]) => `${k}=${v}`)
                .join('&');

            const baseKey = urlObj.pathname + (sortedParams ? '?' + sortedParams : '');

            // Создаем хеш
            let hash = 0;
            for (let i = 0; i < baseKey.length; i++) {
                hash = ((hash << 5) - hash) + baseKey.charCodeAt(i);
                hash = hash & hash;
            }

            const key = `${pageNum}_${Math.abs(hash)}`;

            // Логируем для отладки (только для страницы 1)
            if (pageNum === '1' || pageNum === 1) {
                console.log('AjaxPagination: Cache key for page 1:', {
                    url: url,
                    pageNum: pageNum,
                    originalParams: originalParams,
                    baseKey: baseKey,
                    hash: Math.abs(hash),
                    key: key
                });
            }

            return key;
        } catch (e) {
            console.error('AjaxPagination: Error generating cache key', e);
            return `${pageNum}_${Date.now()}`;
        }
    }

    /**
     * Сохраняет в кэш памяти
     */
    function savePageToCache(key, html, pageNum) {
        try {
            if (pageCache.size >= 20) {
                clearOldestCache();
            }

            pageCache.set(key, {
                html: html,
                pageNum: pageNum,
                timestamp: Date.now(),
                expiry: PAGE_CACHE_EXPIRY
            });

            console.log('AjaxPagination: Saved to cache - key:', key, 'page:', pageNum, 'cache size:', pageCache.size);
        } catch (e) {
            console.warn('AjaxPagination: Cache save failed', e);
        }
    }

    /**
     * Получает из кэша памяти
     */
    function getPageFromCache(key) {
        try {
            const cached = pageCache.get(key);
            if (!cached) {
                return null;
            }

            if (Date.now() - cached.timestamp > cached.expiry) {
                console.log('AjaxPagination: Cache expired for key:', key);
                pageCache.delete(key);
                return null;
            }

            console.log('AjaxPagination: Cache hit for key:', key, 'page:', cached.pageNum);
            return cached;
        } catch (e) {
            console.warn('AjaxPagination: Cache read failed', e);
            return null;
        }
    }

    /**
     * Очищает устаревший кэш
     */
    function clearExpiredCache() {
        try {
            const now = Date.now();
            let expiredCount = 0;
            for (const [key, value] of pageCache.entries()) {
                if (now - value.timestamp > value.expiry) {
                    pageCache.delete(key);
                    expiredCount++;
                }
            }
            if (expiredCount > 0) {
                console.log('AjaxPagination: Cleared', expiredCount, 'expired cache entries');
            }
        } catch {}
    }

    /**
     * Очищает самые старые записи
     */
    function clearOldestCache() {
        try {
            const entries = Array.from(pageCache.entries())
                .map(([key, value]) => ({ key, timestamp: value.timestamp }));

            entries.sort((a, b) => a.timestamp - b.timestamp);

            const deleteCount = Math.max(1, Math.floor(entries.length * 0.2));
            entries.slice(0, deleteCount).forEach(e => {
                pageCache.delete(e.key);
            });

            console.log('AjaxPagination: Cleared', deleteCount, 'oldest cache entries');
        } catch {}
    }

    /**
     * Очищает весь кэш
     */
    function clearPageCache() {
        try {
            const size = pageCache.size;
            pageCache.clear();
            console.log('AjaxPagination: Cache cleared, removed', size, 'entries');
        } catch {}
    }

    /**
     * Обновляет URL
     */
    function updateUrlParams(url, pageNum, replace = false) {
        if (history.pushState) {
            try {
                if (replace) {
                    window.history.replaceState({ path: url, page: pageNum, displayUrl: url }, '', url);
                } else {
                    window.history.pushState({ path: url, page: pageNum, displayUrl: url }, '', url);
                }
                console.log('AjaxPagination: URL updated to:', url);
            } catch {}
        }
    }

    /**
     * Получает page из URL
     */
    function getPageFromUrl(url) {
        try {
            const page = new URL(url, window.location.origin).searchParams.get('page');
            return page || '1';
        } catch {
            const match = url.match(/[?&]page=(\d+)/);
            return match ? match[1] : '1';
        }
    }

    /**
     * Показывает ошибку
     */
    function showErrorMessage(message) {
        const gamesContainer = document.querySelector('.games-container');
        if (!gamesContainer) return;

        let errorDiv = document.querySelector('.ajax-pagination-error');
        if (!errorDiv) {
            errorDiv = document.createElement('div');
            errorDiv.className = 'ajax-pagination-error alert alert-danger alert-dismissible fade show mt-3';
            errorDiv.innerHTML = `
                <strong>Error!</strong> <span class="error-message"></span>
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            `;
            gamesContainer.parentNode.insertBefore(errorDiv, gamesContainer);
        }

        errorDiv.querySelector('.error-message').textContent = message;
        errorDiv.style.display = 'block';
        setTimeout(() => errorDiv.style.display = 'none', 5000);
    }

    // Добавляем стили
    const style = document.createElement('style');
    style.textContent = `
        .games-container.loading {
            opacity: 0.6;
            pointer-events: none;
            transition: opacity 0.3s ease;
            position: relative;
        }

        .loading-text {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            z-index: 1000;
            background: var(--secondary-color);
            color: white;
            padding: 10px 20px;
            border-radius: 30px;
            font-weight: 600;
            box-shadow: 0 4px 15px rgba(255, 107, 53, 0.3);
            animation: pulse 1.5s infinite;
        }

        @keyframes pulse {
            0% { opacity: 0.7; transform: translate(-50%, -50%) scale(1); }
            50% { opacity: 1; transform: translate(-50%, -50%) scale(1.05); }
            100% { opacity: 0.7; transform: translate(-50%, -50%) scale(1); }
        }
    `;
    document.head.appendChild(style);

    // Экспорт в глобальную область
    window.AjaxPagination = {
        loadPage: (ajaxUrl, displayUrl, pageNum) => loadRequestedPage(ajaxUrl, displayUrl, pageNum),
        clearCache: clearPageCache,
        reinitializeComponents: reinitializeComponents,
        getCacheStats: () => ({
            size: pageCache.size,
            keys: Array.from(pageCache.keys())
        })
    };
})();

console.log('AjaxPagination: Script loaded');