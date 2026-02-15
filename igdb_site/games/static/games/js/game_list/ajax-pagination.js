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
}

/**
 * Загружает страницу игр с проверкой кэша
 */
function loadGamesPageWithCache(url, pageNum, isPopState = false) {
    const gamesGridRow = document.getElementById('games-grid-row');
    const gamesContainer = document.querySelector('.games-container');

    if (!gamesGridRow || !gamesContainer) {
        console.error('AjaxPagination: Games grid row not found');
        if (!isPopState) window.location.href = url;
        return;
    }

    // Генерируем ключ кэша
    const cacheKey = generateCacheKey(url, pageNum);

    // Проверяем кэш
    const cachedPage = getPageFromCache(cacheKey);
    if (cachedPage) {
        console.log('AjaxPagination: Loading from cache, page', pageNum);

        // Обновляем сетку игр
        gamesGridRow.innerHTML = cachedPage.html;

        // Обновляем информацию о странице и пагинацию
        updatePageInfo(pageNum);
        updatePaginationActiveState(pageNum);

        // Обновляем URL
        updateUrlParams(url, pageNum);

        // Отправляем событие
        document.dispatchEvent(new CustomEvent('games-grid-updated', {
            detail: { page: pageNum, source: 'cache' }
        }));

        // Предзагружаем соседние страницы
        prefetchAdjacentPages(pageNum, url);

        return;
    }

    console.log('AjaxPagination: Loading from server, page', pageNum);

    // Показываем загрузку
    const currentHeight = gamesContainer.offsetHeight;
    gamesContainer.style.minHeight = currentHeight + 'px';
    gamesContainer.classList.add('loading-visible');

    const loadingText = gamesContainer.querySelector('.loading-text');
    if (loadingText) loadingText.style.display = 'block';

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
        return response.text();
    })
    .then(html => {
        console.log('AjaxPagination: Server response received');

        // Проверяем, не вернулась ли полная страница
        if (isFullPage(html)) {
            console.error('AjaxPagination: Received full page instead of grid');
            const extractedGrid = extractGridFromFullPage(html);
            if (extractedGrid) {
                html = extractedGrid;
            } else {
                throw new Error('Cannot extract grid from full page');
            }
        }

        // Сохраняем в кэш
        savePageToCache(cacheKey, html, pageNum);

        // Обновляем сетку
        gamesGridRow.innerHTML = html;

        // Обновляем информацию о странице и пагинацию
        updatePageInfo(pageNum);
        updatePaginationActiveState(pageNum);

        // Обновляем URL
        updateUrlParams(url, pageNum);

        // Убираем загрузку
        gamesContainer.classList.remove('loading-visible');
        gamesContainer.style.minHeight = '';
        if (loadingText) loadingText.style.display = 'none';

        // Отправляем событие
        document.dispatchEvent(new CustomEvent('games-grid-updated', {
            detail: { page: pageNum, source: 'server' }
        }));

        // Предзагружаем соседние страницы
        prefetchAdjacentPages(pageNum, url);
    })
    .catch(error => {
        console.error('AjaxPagination: Error:', error);

        gamesContainer.classList.remove('loading-visible');
        gamesContainer.style.minHeight = '';
        if (loadingText) loadingText.style.display = 'none';

        if (!isPopState) {
            console.log('AjaxPagination: Falling back to full page load');
            window.location.href = url;
        } else {
            showErrorMessage('Failed to load page. Please try again.');
        }
    });
}

/**
 * Обновляет информацию о странице и состояние кнопок пагинации
 */
function updatePageInfo(pageNum) {
    pageNum = parseInt(pageNum, 10);
    const totalPages = parseInt(document.getElementById('server-total-pages')?.value || '1', 10);

    // Обновляем spans с номером страницы
    document.querySelectorAll('#games-current, #games-current-top').forEach(span => {
        span.textContent = pageNum;
    });

    // Обновляем скрытое поле
    const currentPageInput = document.getElementById('server-current-page');
    if (currentPageInput) currentPageInput.value = pageNum;

    // Вычисляем start/end индексы
    const itemsPerPage = parseInt(document.getElementById('server-items-per-page')?.value || '16', 10);
    const totalCount = parseInt(document.getElementById('server-total-count')?.value || '0', 10);

    const startIndex = (pageNum - 1) * itemsPerPage + 1;
    const endIndex = Math.min(pageNum * itemsPerPage, totalCount);

    // Обновляем spans
    document.querySelectorAll('#games-start, #games-start-top').forEach(span => {
        span.textContent = startIndex;
    });

    document.querySelectorAll('#games-end, #games-end-top').forEach(span => {
        span.textContent = endIndex;
    });

    // Обновляем скрытые поля
    const startInput = document.getElementById('server-start-index');
    const endInput = document.getElementById('server-end-index');
    if (startInput) startInput.value = startIndex;
    if (endInput) endInput.value = endIndex;

    // Обновляем состояние кнопок пагинации
    updatePaginationButtons(pageNum, totalPages);
}

/**
 * Обновляет состояние кнопок пагинации (Prev/Next)
 */
function updatePaginationButtons(currentPage, totalPages) {
    currentPage = parseInt(currentPage, 10);
    totalPages = parseInt(totalPages, 10);

    // Получаем базовый URL без параметра page
    const baseUrl = window.location.href.split('?')[0];
    const searchParams = new URLSearchParams(window.location.search);

    console.log('AjaxPagination: Updating buttons for page', currentPage, 'of', totalPages);

    // Для всех блоков пагинации
    document.querySelectorAll('.games-pagination').forEach(paginationBlock => {
        // Обновляем Prev кнопки
        const prevItems = paginationBlock.querySelectorAll('.page-item:first-child, .page-item:has(.bi-chevron-left)');
        prevItems.forEach(item => {
            if (currentPage <= 1) {
                // Должна быть disabled
                item.classList.add('disabled');
                if (item.querySelector('a')) {
                    const link = item.querySelector('a');
                    const spanHtml = `<span class="page-link">${link.innerHTML}</span>`;
                    item.innerHTML = spanHtml;
                }
            } else {
                // Должна быть активной
                item.classList.remove('disabled');
                // Всегда обновляем ссылку для Prev
                const params = new URLSearchParams(searchParams);
                params.set('page', currentPage - 1);
                const url = baseUrl + '?' + params.toString();

                if (item.querySelector('a')) {
                    const link = item.querySelector('a');
                    link.href = url;
                    link.dataset.page = currentPage - 1;
                } else {
                    const span = item.querySelector('span.page-link');
                    if (span) {
                        const linkHtml = `<a class="page-link ajax-pagination-link" href="${url}" data-page="${currentPage - 1}">${span.innerHTML}</a>`;
                        item.innerHTML = linkHtml;
                    }
                }
            }
        });

        // Обновляем Next кнопки
        const nextItems = paginationBlock.querySelectorAll('.page-item:last-child, .page-item:has(.bi-chevron-right)');
        nextItems.forEach(item => {
            if (currentPage >= totalPages) {
                // Должна быть disabled
                item.classList.add('disabled');
                if (item.querySelector('a')) {
                    const link = item.querySelector('a');
                    const spanHtml = `<span class="page-link">${link.innerHTML}</span>`;
                    item.innerHTML = spanHtml;
                }
            } else {
                // Должна быть активной
                item.classList.remove('disabled');
                // Всегда обновляем ссылку для Next
                const params = new URLSearchParams(searchParams);
                params.set('page', currentPage + 1);
                const url = baseUrl + '?' + params.toString();

                if (item.querySelector('a')) {
                    const link = item.querySelector('a');
                    link.href = url;
                    link.dataset.page = currentPage + 1;
                } else {
                    const span = item.querySelector('span.page-link');
                    if (span) {
                        const linkHtml = `<a class="page-link ajax-pagination-link" href="${url}" data-page="${currentPage + 1}">${span.innerHTML}</a>`;
                        item.innerHTML = linkHtml;
                    }
                }
            }
        });
    });
}

/**
 * Обновляет активное состояние кнопок пагинации (номера страниц)
 */
function updatePaginationActiveState(pageNum) {
    pageNum = parseInt(pageNum, 10);

    // Для всех блоков пагинации
    document.querySelectorAll('.games-pagination').forEach(paginationBlock => {
        // Убираем active класс у всех элементов
        paginationBlock.querySelectorAll('.page-item.active').forEach(item => {
            item.classList.remove('active');
        });

        // Находим кнопку с нужной страницей и делаем её активной
        paginationBlock.querySelectorAll('.ajax-pagination-link').forEach(link => {
            const linkPage = parseInt(link.dataset.page, 10);
            if (linkPage === pageNum) {
                const parentItem = link.closest('.page-item');
                if (parentItem) {
                    parentItem.classList.add('active');
                }
            }
        });

        // Также обрабатываем span (текущая страница без ссылки)
        paginationBlock.querySelectorAll('.page-item span.page-link').forEach(span => {
            const spanText = span.textContent.trim();
            if (!isNaN(parseInt(spanText, 10)) && parseInt(spanText, 10) === pageNum) {
                const parentItem = span.closest('.page-item');
                if (parentItem && !parentItem.classList.contains('active')) {
                    parentItem.classList.add('active');
                }
            }
        });
    });

    console.log('AjaxPagination: Updated pagination active state to page', pageNum);
}

/**
 * Проверяет, является ли HTML полной страницей
 */
function isFullPage(html) {
    return html.includes('<!DOCTYPE') ||
           html.includes('<html') ||
           html.includes('<body') ||
           (html.includes('games-pagination') && html.includes('navbar'));
}

/**
 * Извлекает сетку игр из полной страницы
 */
function extractGridFromFullPage(html) {
    try {
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = html;

        const gridRow = tempDiv.querySelector('#games-grid-row');
        if (gridRow) {
            return gridRow.innerHTML;
        }

        const gamesContainer = tempDiv.querySelector('.games-container .row');
        if (gamesContainer) {
            return gamesContainer.innerHTML;
        }
    } catch (e) {
        console.error('AjaxPagination: Error extracting grid', e);
    }
    return null;
}

/**
 * Предзагружает соседние страницы
 */
function prefetchAdjacentPages(currentPage, baseUrl) {
    const totalPagesInput = document.getElementById('server-total-pages');
    if (!totalPagesInput) return;

    const totalPages = parseInt(totalPagesInput.value, 10);
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
}

/**
 * Предзагружает одну страницу
 */
function prefetchSinglePage(pageNum, baseUrl) {
    // Исправляем формирование URL для предзагрузки
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
            const extracted = extractGridFromFullPage(html);
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
        return `${PAGE_CACHE_PREFIX}${pageNum}_${btoa(baseKey).replace(/=/g, '')}`;
    } catch {
        return `${PAGE_CACHE_PREFIX}${pageNum}_${url.replace(/[^a-zA-Z0-9]/g, '_')}`;
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

// Экспорт
window.AjaxPagination = {
    loadPage: (url, pageNum) => loadGamesPageWithCache(url, pageNum),
    init: initAjaxPagination,
    clearCache: clearPageCache
};

console.log('AjaxPagination: Script loaded');