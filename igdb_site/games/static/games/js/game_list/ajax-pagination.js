// games/static/games/js/game_list/ajax-pagination.js
// AJAX пагинация для списка игр с предзагрузкой и кэшированием соседних страниц

document.addEventListener('DOMContentLoaded', function() {
    console.log('AjaxPagination: Initializing');
    initAjaxPagination();

    // Кэшируем текущую страницу сразу после загрузки
    setTimeout(() => {
        cacheCurrentPage();
    }, 100);
});

// Простой кэш в памяти (не в sessionStorage)
const pageCache = new Map();
const PAGE_CACHE_EXPIRY = 30 * 60 * 1000; // 30 минут

// Базовый URL для AJAX запросов
const AJAX_URL = '/ajax/load-games-page/';

function initAjaxPagination() {
    console.log('AjaxPagination: Setting up event listeners');

    // Очищаем устаревший кэш
    clearExpiredCache();

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

        // Строим красивый URL для адресной строки
        const displayUrl = buildDisplayUrl(params, pageNum);

        console.log('AjaxPagination: AJAX URL:', ajaxUrl);
        console.log('AjaxPagination: Display URL:', displayUrl);

        loadGamesPageWithCache(ajaxUrl, displayUrl, pageNum);
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

            loadGamesPageWithCache(ajaxUrl, e.state.displayUrl, pageNum, true);
        }
    });

    // Проверяем, что total_pages правильно установлено после загрузки
    setTimeout(checkTotalPages, 500);
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
 */
function buildDisplayUrl(params, pageNum) {
    // Копируем параметры
    const displayParams = new URLSearchParams(params);

    // Устанавливаем page
    displayParams.set('page', pageNum);

    // Строим URL
    return '/games/?' + displayParams.toString();
}

/**
 * Кэширует текущую страницу
 */
function cacheCurrentPage() {
    const gamesResultsContainer = document.getElementById('games-results-container');
    const currentPageInput = document.getElementById('server-current-page');
    const currentUrl = window.location.href;

    if (!gamesResultsContainer || !currentPageInput) {
        console.log('AjaxPagination: Cannot cache current page - elements not found');
        return;
    }

    const currentPage = currentPageInput.value;
    if (!currentPage) {
        console.log('AjaxPagination: Cannot cache current page - page number not found');
        return;
    }

    // Для текущей страницы используем displayUrl как ключ
    const displayUrl = currentUrl;
    const cacheKey = generateCacheKey(displayUrl, currentPage);
    const currentHtml = gamesResultsContainer.innerHTML;

    // Проверяем, не закэширована ли уже страница
    if (!getPageFromCache(cacheKey)) {
        savePageToCache(cacheKey, currentHtml, currentPage);
        console.log('AjaxPagination: Cached current page', currentPage, 'with key:', cacheKey);
    }
}

/**
 * Переинициализирует все компоненты после загрузки нового контента
 */
function reinitializeComponents() {
    console.log('AjaxPagination: Reinitializing components');

    // Переинициализируем Bootstrap tooltips
    if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
        const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
        tooltipTriggerList.map(function (tooltipTriggerEl) {
            // Удаляем существующий tooltip если есть
            const tooltip = bootstrap.Tooltip.getInstance(tooltipTriggerEl);
            if (tooltip) {
                tooltip.dispose();
            }
            return new bootstrap.Tooltip(tooltipTriggerEl);
        });
        console.log('AjaxPagination: Reinitialized', tooltipTriggerList.length, 'tooltips');
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

    // Применяем кастомные стили для рейтингов (svg)
    document.querySelectorAll('.rating-star-svg, .similarity-pattern-svg').forEach(svg => {
        // Убеждаемся что svg отображается корректно
        svg.style.display = 'block';
    });

    // Перезапускаем similarity-compare кнопки если они есть
    if (window.SimilarityCompare && typeof window.SimilarityCompare.init === 'function') {
        window.SimilarityCompare.init();
    }

    // Обновляем индикатор режима похожих игр
    const similarityModeIndicator = document.querySelector('.similarity-mode-indicator');
    if (similarityModeIndicator) {
        const mode = similarityModeIndicator.dataset.mode;
        if (mode === 'similar') {
            document.body.classList.add('similarity-mode');
        }
    }

    // Применяем стили для платформ (platform badges)
    document.querySelectorAll('.platforms-overlay .badge').forEach(badge => {
        // Убеждаемся что бейджи отображаются правильно
        badge.style.fontSize = '0.7rem';
    });

    // Вызываем событие для других модулей
    document.dispatchEvent(new CustomEvent('ajax-content-loaded'));
}

/**
 * Загружает страницу игр с проверкой кэша
 */
function loadGamesPageWithCache(ajaxUrl, displayUrl, pageNum, isPopState = false) {
    const gamesResultsContainer = document.getElementById('games-results-container');

    if (!gamesResultsContainer) {
        console.error('AjaxPagination: Games results container not found');
        if (!isPopState) window.location.href = displayUrl;
        return;
    }

    // Генерируем ключ кэша на основе displayUrl (чтобы кэш был привязан к красивому URL)
    const cacheKey = generateCacheKey(displayUrl, pageNum);

    // Проверяем кэш в памяти
    const cachedPage = getPageFromCache(cacheKey);
    if (cachedPage) {
        console.log('AjaxPagination: Loading from memory cache, page', pageNum);

        // Обновляем весь контейнер результатов
        gamesResultsContainer.innerHTML = cachedPage.html;

        // Обновляем URL на красивый
        updateUrlParams(displayUrl, pageNum);

        // Переинициализируем компоненты
        reinitializeComponents();

        // Отправляем событие
        document.dispatchEvent(new CustomEvent('games-grid-updated', {
            detail: { page: pageNum, source: 'cache' }
        }));

        // Предзагружаем соседние страницы
        setTimeout(() => {
            prefetchAdjacentPages(pageNum, displayUrl);
        }, 100);

        return;
    }

    console.log('AjaxPagination: Loading from server, page', pageNum, 'URL:', ajaxUrl);

    // Показываем загрузку
    showLoading();

    // Загружаем с сервера
    fetch(ajaxUrl, {
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'text/html'
        },
        credentials: 'same-origin'
    })
    .then(response => {
        if (!response.ok) throw new Error(`HTTP error ${response.status}`);

        // Получаем total-pages из заголовка
        const totalPagesHeader = response.headers.get('X-Total-Pages');
        if (totalPagesHeader) {
            console.log('AjaxPagination: Server total pages from header:', totalPagesHeader);
        }

        return response.text();
    })
    .then(html => {
        console.log('AjaxPagination: Server response received');

        // Проверяем, не вернулась ли полная страница
        if (isFullPage(html)) {
            console.warn('AjaxPagination: Received full page, extracting results container');
            const extractedResults = extractResultsFromFullPage(html);
            if (extractedResults) {
                html = extractedResults;
            } else {
                throw new Error('Cannot extract results from full page');
            }
        }

        // Сохраняем в кэш памяти с ключом от displayUrl
        savePageToCache(cacheKey, html, pageNum);

        // Обновляем весь контейнер результатов
        gamesResultsContainer.innerHTML = html;

        // Убираем загрузку
        hideLoading();

        // Обновляем URL на красивый
        updateUrlParams(displayUrl, pageNum);

        // Переинициализируем компоненты
        reinitializeComponents();

        // Отправляем событие
        document.dispatchEvent(new CustomEvent('games-grid-updated', {
            detail: { page: pageNum, source: 'server' }
        }));

        // Проверяем total_pages после обновления DOM
        setTimeout(() => {
            checkTotalPages();
            prefetchAdjacentPages(pageNum, displayUrl);
        }, 100);
    })
    .catch(error => {
        console.error('AjaxPagination: Error:', error);

        hideLoading();

        if (!isPopState) {
            console.log('AjaxPagination: Falling back to full page load');
            window.location.href = displayUrl;
        } else {
            showErrorMessage('Failed to load page. Please try again.');
        }
    });
}

/**
 * Показывает индикатор загрузки
 */
function showLoading() {
    // Показываем текст загрузки
    const loadingText = document.querySelector('.loading-text');
    if (loadingText) {
        loadingText.style.display = 'block';
    }

    // Добавляем класс loading к контейнеру для затемнения
    const gamesContainer = document.querySelector('.games-container');
    if (gamesContainer) {
        gamesContainer.classList.add('loading');
    }

    // Блокируем кнопки пагинации
    document.querySelectorAll('.ajax-pagination-link').forEach(link => {
        link.style.pointerEvents = 'none';
        link.style.opacity = '0.6';
    });
}

/**
 * Скрывает индикатор загрузки
 */
function hideLoading() {
    // Скрываем текст загрузки
    const loadingText = document.querySelector('.loading-text');
    if (loadingText) {
        loadingText.style.display = 'none';
    }

    // Убираем класс loading
    const gamesContainer = document.querySelector('.games-container');
    if (gamesContainer) {
        gamesContainer.classList.remove('loading');
    }

    // Разблокируем кнопки пагинации
    document.querySelectorAll('.ajax-pagination-link').forEach(link => {
        link.style.pointerEvents = 'auto';
        link.style.opacity = '1';
    });
}

/**
 * Проверяет, является ли HTML полной страницей
 */
function isFullPage(html) {
    return html.includes('<!DOCTYPE') ||
           html.includes('<html') ||
           html.includes('<body') ||
           (html.includes('navbar') && html.includes('footer'));
}

/**
 * Извлекает контейнер результатов из полной страницы
 */
function extractResultsFromFullPage(html) {
    try {
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = html;

        const resultsContainer = tempDiv.querySelector('#games-results-container');
        if (resultsContainer) {
            return resultsContainer.innerHTML;
        }

        // Fallback: ищем блок с классом row (первый после контейнера)
        const row = tempDiv.querySelector('.container .row');
        if (row) {
            return row.innerHTML;
        }
    } catch (e) {
        console.error('AjaxPagination: Error extracting results', e);
    }
    return null;
}

/**
 * Предзагружает соседние страницы
 */
function prefetchAdjacentPages(currentPage, baseDisplayUrl) {
    // Получаем total-pages из DOM после обновления
    setTimeout(() => {
        const totalPagesInput = document.getElementById('server-total-pages');
        if (!totalPagesInput) {
            console.warn('AjaxPagination: No server-total-pages input found');
            return;
        }

        const totalPages = parseInt(totalPagesInput.value, 10);
        console.log('AjaxPagination: Total pages for prefetch:', totalPages);

        if (isNaN(totalPages) || totalPages <= 1) return;

        currentPage = parseInt(currentPage, 10);
        const pagesToPrefetch = [];

        // 2 предыдущие
        for (let i = 1; i <= 2; i++) {
            if (currentPage - i >= 1) pagesToPrefetch.push(currentPage - i);
        }

        // 2 следующие
        for (let i = 1; i <= 2; i++) {
            if (currentPage + i <= totalPages) pagesToPrefetch.push(currentPage + i);
        }

        console.log('AjaxPagination: Prefetching pages', pagesToPrefetch);

        pagesToPrefetch.forEach((page, index) => {
            setTimeout(() => {
                prefetchSinglePage(page, baseDisplayUrl);
            }, index * 300);
        });
    }, 100);
}

/**
 * Предзагружает одну страницу
 */
function prefetchSinglePage(pageNum, baseDisplayUrl) {
    // Извлекаем параметры из baseDisplayUrl
    const params = extractParamsFromUrl(baseDisplayUrl);

    // Строим AJAX URL
    const ajaxUrl = buildAjaxUrl(params, pageNum);

    // Строим display URL для ключа кэша
    const displayUrl = buildDisplayUrl(params, pageNum);

    const cacheKey = generateCacheKey(displayUrl, pageNum);

    if (getPageFromCache(cacheKey)) return;

    fetch(ajaxUrl, {
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'same-origin'
    })
    .then(response => response.ok ? response.text() : null)
    .then(html => {
        if (!html) return;

        if (isFullPage(html)) {
            const extracted = extractResultsFromFullPage(html);
            if (extracted) html = extracted;
        }

        savePageToCache(cacheKey, html, pageNum);
        console.log('AjaxPagination: Prefetched page', pageNum);
    })
    .catch(() => {});
}

/**
 * Генерирует ключ кэша
 */
function generateCacheKey(url, pageNum) {
    try {
        const urlObj = new URL(url, window.location.origin);
        const params = new URLSearchParams(urlObj.search);
        params.delete('page');

        const sortedParams = Array.from(params.entries())
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([k, v]) => `${k}=${v}`)
            .join('&');

        const baseKey = urlObj.pathname + (sortedParams ? '?' + sortedParams : '');
        // Используем простой хеш для ключа
        let hash = 0;
        for (let i = 0; i < baseKey.length; i++) {
            hash = ((hash << 5) - hash) + baseKey.charCodeAt(i);
            hash = hash & hash;
        }
        return `${pageNum}_${Math.abs(hash)}`;
    } catch {
        return `${pageNum}_${Date.now()}`;
    }
}

/**
 * Сохраняет в кэш памяти
 */
function savePageToCache(key, html, pageNum) {
    try {
        // Проверяем размер кэша и очищаем если нужно
        if (pageCache.size >= 20) {
            clearOldestCache();
        }

        pageCache.set(key, {
            html: html,
            pageNum: pageNum,
            timestamp: Date.now(),
            expiry: PAGE_CACHE_EXPIRY
        });

        console.log('AjaxPagination: Saved page', pageNum, 'to cache with key:', key, 'Total cache size:', pageCache.size);
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
        if (!cached) return null;

        if (Date.now() - cached.timestamp > cached.expiry) {
            pageCache.delete(key);
            return null;
        }

        return cached;
    } catch {
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
        // Преобразуем Map в массив для сортировки
        const entries = Array.from(pageCache.entries())
            .map(([key, value]) => ({ key, timestamp: value.timestamp }));

        entries.sort((a, b) => a.timestamp - b.timestamp);

        // Удаляем 20% самых старых записей (минимум 1)
        const deleteCount = Math.max(1, Math.floor(entries.length * 0.2));
        entries.slice(0, deleteCount).forEach(e => {
            pageCache.delete(e.key);
        });

        console.log('AjaxPagination: Cleared', deleteCount, 'oldest cache entries to free space');
    } catch {}
}

/**
 * Очищает весь кэш
 */
function clearPageCache() {
    try {
        const cacheSize = pageCache.size;
        pageCache.clear();
        console.log('AjaxPagination: Cleared', cacheSize, 'cache entries');
    } catch {}
}

/**
 * Обновляет URL (только красивый URL)
 */
function updateUrlParams(url, pageNum) {
    if (history.pushState) {
        try {
            window.history.pushState({ path: url, page: pageNum, displayUrl: url }, '', url);
            console.log('AjaxPagination: Updated URL to:', url);
        } catch {}
    }
}

/**
 * Получает page из URL
 */
function getPageFromUrl(url) {
    try {
        return new URL(url, window.location.origin).searchParams.get('page') || '1';
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

// Добавляем стили для загрузки
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

    .ajax-pagination-link {
        transition: opacity 0.3s ease;
    }
`;
document.head.appendChild(style);

// Экспорт
window.AjaxPagination = {
    loadPage: (ajaxUrl, displayUrl, pageNum) => loadGamesPageWithCache(ajaxUrl, displayUrl, pageNum),
    init: initAjaxPagination,
    clearCache: clearPageCache,
    checkTotalPages: checkTotalPages,
    reinitializeComponents: reinitializeComponents,
    cacheCurrentPage: cacheCurrentPage
};

console.log('AjaxPagination: Script loaded');