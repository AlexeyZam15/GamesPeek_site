// games/static/games/js/game_list/ajax-pagination.js
// AJAX пагинация для списка игр с предзагрузкой и кэшированием соседних страниц

document.addEventListener('DOMContentLoaded', function() {
    console.log('AjaxPagination: Initializing');
    initAjaxPagination();
});

// Константы для кэширования
const PAGE_CACHE_PREFIX = 'games_page_';
const PAGE_CACHE_EXPIRY = 30 * 60 * 1000; // 30 минут

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

        const url = target.getAttribute('href');
        const pageNum = target.dataset.page;

        if (!url) {
            console.error('AjaxPagination: No URL found in link');
            return;
        }

        console.log('AjaxPagination: Loading page', pageNum, 'URL:', url);
        loadGamesPageWithCache(url, pageNum);
    });

    // Обработка кнопок назад/вперед
    window.addEventListener('popstate', function(e) {
        if (e.state && e.state.path) {
            console.log('AjaxPagination: Popstate event');
            const pageNum = getPageFromUrl(e.state.path);
            loadGamesPageWithCache(e.state.path, pageNum, true);
        }
    });

    // Проверяем, что total_pages правильно установлено после загрузки
    setTimeout(checkTotalPages, 500);
}

/**
 * Проверяет правильность установки total_pages
 */
function checkTotalPages() {
    const totalPagesInput = document.getElementById('server-total-pages');
    if (totalPagesInput) {
        const totalPages = parseInt(totalPagesInput.value, 10);
        console.log('AjaxPagination: Total pages from server:', totalPages);

        // Проверяем, отображается ли последняя страница в пагинации
        const paginationLinks = document.querySelectorAll('.ajax-pagination-link[data-page]');
        let maxPage = 0;
        paginationLinks.forEach(link => {
            const page = parseInt(link.dataset.page, 10);
            if (!isNaN(page) && page > maxPage) {
                maxPage = page;
            }
        });

        console.log('AjaxPagination: Max page in pagination:', maxPage);

        if (totalPages > maxPage) {
            console.warn('AjaxPagination: Last page', totalPages, 'not found in pagination! Max found:', maxPage);
        }
    }
}

/**
 * Загружает страницу игр с проверкой кэша
 */
function loadGamesPageWithCache(url, pageNum, isPopState = false) {
    const gamesResultsContainer = document.getElementById('games-results-container');

    if (!gamesResultsContainer) {
        console.error('AjaxPagination: Games results container not found');
        if (!isPopState) window.location.href = url;
        return;
    }

    // Генерируем ключ кэша
    const cacheKey = generateCacheKey(url, pageNum);

    // Проверяем кэш
    const cachedPage = getPageFromCache(cacheKey);
    if (cachedPage) {
        console.log('AjaxPagination: Loading from cache, page', pageNum);

        // Обновляем весь контейнер результатов
        gamesResultsContainer.innerHTML = cachedPage.html;

        // Обновляем URL
        updateUrlParams(url, pageNum);

        // Отправляем событие
        document.dispatchEvent(new CustomEvent('games-grid-updated', {
            detail: { page: pageNum, source: 'cache' }
        }));

        // Предзагружаем соседние страницы
        setTimeout(() => {
            prefetchAdjacentPages(pageNum, url);
        }, 100);

        return;
    }

    console.log('AjaxPagination: Loading from server, page', pageNum);

    // Показываем загрузку
    showLoading();

    // Загружаем с сервера
    fetch(url, {
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

        // Сохраняем в кэш
        savePageToCache(cacheKey, html, pageNum);

        // Обновляем весь контейнер результатов
        gamesResultsContainer.innerHTML = html;

        // Убираем загрузку
        hideLoading();

        // Обновляем URL
        updateUrlParams(url, pageNum);

        // Отправляем событие
        document.dispatchEvent(new CustomEvent('games-grid-updated', {
            detail: { page: pageNum, source: 'server' }
        }));

        // Проверяем total_pages после обновления DOM
        setTimeout(() => {
            checkTotalPages();
            prefetchAdjacentPages(pageNum, url);
        }, 100);
    })
    .catch(error => {
        console.error('AjaxPagination: Error:', error);

        hideLoading();

        if (!isPopState) {
            console.log('AjaxPagination: Falling back to full page load');
            window.location.href = url;
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
function prefetchAdjacentPages(currentPage, baseUrl) {
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
                prefetchSinglePage(page, baseUrl);
            }, index * 300);
        });
    }, 100);
}

/**
 * Предзагружает одну страницу
 */
function prefetchSinglePage(pageNum, baseUrl) {
    const baseUrlWithoutPage = baseUrl.split('?')[0];
    const params = new URLSearchParams(baseUrl.includes('?') ? baseUrl.split('?')[1] : '');
    params.set('page', pageNum);
    const url = baseUrlWithoutPage + '?' + params.toString();

    const cacheKey = generateCacheKey(url, pageNum);

    if (getPageFromCache(cacheKey)) return;

    fetch(url, {
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
        // Используем простой хеш вместо btoa для кириллицы
        let hash = 0;
        for (let i = 0; i < baseKey.length; i++) {
            hash = ((hash << 5) - hash) + baseKey.charCodeAt(i);
            hash = hash & hash;
        }
        return `${PAGE_CACHE_PREFIX}${pageNum}_${Math.abs(hash)}`;
    } catch {
        return `${PAGE_CACHE_PREFIX}${pageNum}_${Date.now()}`;
    }
}

/**
 * Сохраняет в кэш
 */
function savePageToCache(key, html, pageNum) {
    try {
        sessionStorage.setItem(key, JSON.stringify({
            html: html,
            pageNum: pageNum,
            timestamp: Date.now(),
            expiry: PAGE_CACHE_EXPIRY
        }));
    } catch (e) {
        if (e.name === 'QuotaExceededError') {
            clearOldestCache();
            try {
                sessionStorage.setItem(key, JSON.stringify({
                    html,
                    pageNum,
                    timestamp: Date.now(),
                    expiry: PAGE_CACHE_EXPIRY
                }));
            } catch {}
        }
    }
}

/**
 * Получает из кэша
 */
function getPageFromCache(key) {
    try {
        const cached = sessionStorage.getItem(key);
        if (!cached) return null;

        const item = JSON.parse(cached);
        if (Date.now() - item.timestamp > item.expiry) {
            sessionStorage.removeItem(key);
            return null;
        }
        return item;
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
        for (let i = 0; i < sessionStorage.length; i++) {
            const key = sessionStorage.key(i);
            if (key?.startsWith(PAGE_CACHE_PREFIX)) {
                try {
                    const item = JSON.parse(sessionStorage.getItem(key));
                    if (now - item.timestamp > item.expiry) {
                        sessionStorage.removeItem(key);
                    }
                } catch {
                    sessionStorage.removeItem(key);
                }
            }
        }
    } catch {}
}

/**
 * Очищает самые старые записи
 */
function clearOldestCache() {
    try {
        const entries = [];
        for (let i = 0; i < sessionStorage.length; i++) {
            const key = sessionStorage.key(i);
            if (key?.startsWith(PAGE_CACHE_PREFIX)) {
                try {
                    const item = JSON.parse(sessionStorage.getItem(key));
                    entries.push({ key, timestamp: item.timestamp });
                } catch {
                    sessionStorage.removeItem(key);
                }
            }
        }

        entries.sort((a, b) => a.timestamp - b.timestamp);
        entries.slice(0, Math.max(1, Math.floor(entries.length * 0.2))).forEach(e => {
            sessionStorage.removeItem(e.key);
        });
    } catch {}
}

/**
 * Очищает весь кэш
 */
function clearPageCache() {
    try {
        const keys = [];
        for (let i = 0; i < sessionStorage.length; i++) {
            const key = sessionStorage.key(i);
            if (key?.startsWith(PAGE_CACHE_PREFIX)) keys.push(key);
        }
        keys.forEach(key => sessionStorage.removeItem(key));
        console.log('AjaxPagination: Cleared', keys.length, 'cache entries');
    } catch {}
}

/**
 * Обновляет URL
 */
function updateUrlParams(url, pageNum) {
    if (history.pushState) {
        try {
            window.history.pushState({ path: url, page: pageNum }, '', url);
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
    loadPage: (url, pageNum) => loadGamesPageWithCache(url, pageNum),
    init: initAjaxPagination,
    clearCache: clearPageCache,
    checkTotalPages: checkTotalPages
};

console.log('AjaxPagination: Script loaded');