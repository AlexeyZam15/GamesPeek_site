// games/static/games/js/game_list/ajax-pagination.js
// AJAX пагинация для списка игр

document.addEventListener('DOMContentLoaded', function() {
    console.log('AjaxPagination: Initializing');
    initAjaxPagination();
});

function initAjaxPagination() {
    console.log('AjaxPagination: Setting up event listeners');

    // Используем делегирование событий на документе
    document.addEventListener('click', function(e) {
        const target = e.target.closest('.ajax-pagination-link');
        if (!target) return;

        // Проверяем, не является ли родительский элемент disabled
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
        loadGamesPage(url, pageNum);
    });

    // Обработка кнопок назад/вперед в браузере
    window.addEventListener('popstate', function(e) {
        if (e.state && e.state.path) {
            console.log('AjaxPagination: Popstate event, loading:', e.state.path);
            loadGamesPage(e.state.path, getPageFromUrl(e.state.path));
        }
    });
}

function loadGamesPage(url, pageNum) {
    const gamesContainer = document.querySelector('.games-container');
    const gamesGridRow = document.getElementById('games-grid-row');
    const resultsContainer = document.getElementById('games-results-container');

    if (!gamesContainer) {
        console.error('AjaxPagination: Games container not found');
        return;
    }

    if (!gamesGridRow) {
        console.error('AjaxPagination: Games grid row not found');
        return;
    }

    console.log('AjaxPagination: Loading page', pageNum);

    // Сохраняем текущую высоту контейнера чтобы предотвратить скачки
    const currentHeight = gamesContainer.offsetHeight;
    gamesContainer.style.minHeight = currentHeight + 'px';

    // Показываем индикатор загрузки
    gamesContainer.classList.add('loading-visible');

    // Показываем текст загрузки
    const loadingText = gamesContainer.querySelector('.loading-text');
    if (loadingText) {
        loadingText.style.display = 'block';
    }

    // Загружаем новую страницу
    fetch(url, {
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'text/html, application/xhtml+xml'
        },
        credentials: 'same-origin'
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`Network response was not ok: ${response.status} ${response.statusText}`);
        }
        return response.text();
    })
    .then(html => {
        console.log('AjaxPagination: Page loaded successfully, updating DOM');

        // Создаем временный DOM элемент для парсинга
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = html;

        // 1. Обновляем сетку игр
        const newGridRow = tempDiv.querySelector('#games-grid-row');
        if (newGridRow) {
            gamesGridRow.innerHTML = newGridRow.innerHTML;
        } else {
            console.warn('AjaxPagination: Could not find games grid in response');
        }

        // 2. Обновляем пагинацию - заменяем оба блока пагинации
        const paginationBlocks = document.querySelectorAll('.games-pagination');
        const newPaginationBlocks = tempDiv.querySelectorAll('.games-pagination');

        paginationBlocks.forEach((oldBlock, index) => {
            if (newPaginationBlocks[index]) {
                oldBlock.innerHTML = newPaginationBlocks[index].innerHTML;
            }
        });

        // 3. Обновляем скрытые поля с серверными данными
        const serverCurrentPage = document.getElementById('server-current-page');
        const newServerCurrentPage = tempDiv.querySelector('#server-current-page');
        if (serverCurrentPage && newServerCurrentPage) {
            serverCurrentPage.value = newServerCurrentPage.value;
        }

        const serverTotalPages = document.getElementById('server-total-pages');
        const newServerTotalPages = tempDiv.querySelector('#server-total-pages');
        if (serverTotalPages && newServerTotalPages) {
            serverTotalPages.value = newServerTotalPages.value;
        }

        const serverStartIndex = document.getElementById('server-start-index');
        const newServerStartIndex = tempDiv.querySelector('#server-start-index');
        if (serverStartIndex && newServerStartIndex) {
            serverStartIndex.value = newServerStartIndex.value;
        }

        const serverEndIndex = document.getElementById('server-end-index');
        const newServerEndIndex = tempDiv.querySelector('#server-end-index');
        if (serverEndIndex && newServerEndIndex) {
            serverEndIndex.value = newServerEndIndex.value;
        }

        // 4. Обновляем текстовую информацию о странице
        const currentPageSpans = document.querySelectorAll('#games-current, #games-current-top');
        currentPageSpans.forEach(span => {
            span.textContent = pageNum;
        });

        // Убираем класс загрузки и сбрасываем min-height
        gamesContainer.classList.remove('loading-visible');

        // Скрываем текст загрузки
        const loadingText = gamesContainer.querySelector('.loading-text');
        if (loadingText) {
            loadingText.style.display = 'none';
        }
        gamesContainer.style.minHeight = '';

        // Обновляем URL в адресной строке
        updateUrlParams(url, pageNum);

        // Отправляем события для обновления компонентов
        document.dispatchEvent(new CustomEvent('games-grid-updated', {
            detail: { page: pageNum, source: 'ajax-pagination' }
        }));

        // Инициализируем тултипы Bootstrap для новых элементов
        if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
            try {
                const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
                tooltipTriggerList.map(function (tooltipTriggerEl) {
                    return new bootstrap.Tooltip(tooltipTriggerEl);
                });
            } catch (e) {
                console.warn('AjaxPagination: Error initializing tooltips:', e);
            }
        }

        console.log('AjaxPagination: Page', pageNum, 'loaded successfully');
    })
    .catch(error => {
        console.error('AjaxPagination: Error loading page:', error);

        // Убираем класс загрузки в случае ошибки
        gamesContainer.classList.remove('loading-visible');

        // Скрываем текст загрузки
        const loadingText = gamesContainer.querySelector('.loading-text');
        if (loadingText) {
            loadingText.style.display = 'none';
        }
        gamesContainer.style.minHeight = '';

        // Показываем сообщение об ошибке
        showErrorMessage('Failed to load page. Please try again.');
    });
}

function updateUrlParams(url, pageNum) {
    // Обновляем URL в адресной строке без перезагрузки
    if (history.pushState) {
        try {
            window.history.pushState({ path: url, page: pageNum }, '', url);
            console.log('AjaxPagination: URL updated to:', url);
        } catch (e) {
            console.error('AjaxPagination: Failed to update URL:', e);
        }
    }
}

function updatePageText(pageNum) {
    // Обновляем только текстовую информацию о странице
    const currentPageSpans = document.querySelectorAll('#games-current, #games-current-top');
    currentPageSpans.forEach(span => {
        span.textContent = pageNum;
    });
}

function getPageFromUrl(url) {
    try {
        const urlObj = new URL(url, window.location.origin);
        return urlObj.searchParams.get('page') || '1';
    } catch (e) {
        const match = url.match(/[?&]page=(\d+)/);
        return match ? match[1] : '1';
    }
}

function showErrorMessage(message) {
    const gamesContainer = document.querySelector('.games-container');
    if (!gamesContainer) return;

    let errorDiv = document.querySelector('.ajax-pagination-error');

    if (!errorDiv) {
        errorDiv = document.createElement('div');
        errorDiv.className = 'ajax-pagination-error alert alert-danger alert-dismissible fade show mt-3';
        errorDiv.setAttribute('role', 'alert');
        errorDiv.innerHTML = `
            <strong>Error!</strong> <span class="error-message"></span>
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        `;
        gamesContainer.parentNode.insertBefore(errorDiv, gamesContainer);
    }

    errorDiv.querySelector('.error-message').textContent = message;
    errorDiv.style.display = 'block';

    setTimeout(() => {
        if (errorDiv) {
            errorDiv.style.display = 'none';
        }
    }, 5000);
}

// Экспортируем
window.AjaxPagination = {
    loadPage: loadGamesPage,
    init: initAjaxPagination
};

console.log('AjaxPagination: Script loaded');