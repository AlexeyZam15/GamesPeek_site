// games/static/games/js/game_list/ajax-pagination.js
// AJAX пагинация для списка игр

document.addEventListener('DOMContentLoaded', function() {
    console.log('AjaxPagination: Initializing');
    initAjaxPagination();
});

function initAjaxPagination() {
    console.log('AjaxPagination: Setting up event listeners');

    // Используем делегирование событий для всех ссылок пагинации
    document.addEventListener('click', function(e) {
        const target = e.target.closest('.ajax-pagination-link');
        if (!target) return;

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
        } else {
            // Если нет state, пробуем получить из текущего URL
            const currentUrl = window.location.href;
            console.log('AjaxPagination: Popstate event, loading from current URL:', currentUrl);
            loadGamesPage(currentUrl, getPageFromUrl(currentUrl));
        }
    });
}

function loadGamesPage(url, pageNum) {
    const gamesContainer = document.querySelector('.games-container');
    const resultsContainer = document.getElementById('games-results-container');

    if (!gamesContainer) {
        console.error('AjaxPagination: Games container not found');
        return;
    }

    if (!resultsContainer) {
        console.error('AjaxPagination: Results container not found');
        return;
    }

    console.log('AjaxPagination: Loading page', pageNum);

    // Добавляем класс загрузки
    gamesContainer.classList.add('loading');

    // Добавляем класс загрузки на контейнер пагинации
    const paginationContainer = document.querySelector('.games-pagination');
    if (paginationContainer) {
        paginationContainer.classList.add('pagination-loading');
    }

    // Добавляем индикатор загрузки на ссылки
    document.querySelectorAll('.ajax-pagination-link').forEach(link => {
        if (link.dataset.page === pageNum) {
            link.classList.add('loading');
            // Сохраняем оригинальный текст для восстановления
            if (!link.dataset.originalText) {
                link.dataset.originalText = link.innerHTML;
            }
            link.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Loading...';
        }
    });

    // Обновляем URL в адресной строке без перезагрузки
    updateUrlParams(url, pageNum);

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

        // Проверяем тип контента
        const contentType = response.headers.get('content-type');
        if (!contentType || !contentType.includes('text/html')) {
            console.warn('AjaxPagination: Unexpected content type:', contentType);
        }

        return response.text();
    })
    .then(html => {
        console.log('AjaxPagination: Page loaded successfully, updating DOM');

        // Обновляем контейнер с результатами
        resultsContainer.innerHTML = html;

        // Убираем класс загрузки
        gamesContainer.classList.remove('loading');

        // Убираем класс загрузки с пагинации
        if (paginationContainer) {
            paginationContainer.classList.remove('pagination-loading');
        }

        // Восстанавливаем ссылки
        document.querySelectorAll('.ajax-pagination-link.loading').forEach(link => {
            link.classList.remove('loading');
            if (link.dataset.originalText) {
                link.innerHTML = link.dataset.originalText;
                delete link.dataset.originalText;
            }
        });

        // Прокручиваем к началу сетки игр
        const gamesGridRow = document.getElementById('games-grid-row');
        if (gamesGridRow) {
            gamesGridRow.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }

        // Отправляем события для обновления компонентов
        console.log('AjaxPagination: Dispatching update events');
        document.dispatchEvent(new CustomEvent('games-grid-updated', {
            detail: { page: pageNum, source: 'ajax-pagination' }
        }));
        document.dispatchEvent(new CustomEvent('page-loaded', {
            detail: { page: pageNum, url: url }
        }));

        // Обновляем активную страницу в пагинации
        updateActivePageInPagination(pageNum);

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

        gamesContainer.classList.remove('loading');

        if (paginationContainer) {
            paginationContainer.classList.remove('pagination-loading');
        }

        // Восстанавливаем ссылки
        document.querySelectorAll('.ajax-pagination-link.loading').forEach(link => {
            link.classList.remove('loading');
            if (link.dataset.originalText) {
                link.innerHTML = link.dataset.originalText;
                delete link.dataset.originalText;
            }
        });

        // Показываем сообщение об ошибке
        showErrorMessage('Failed to load page. Please try again.');
    });
}

function updateUrlParams(url, pageNum) {
    // Обновляем URL в адресной строке без перезагрузки
    if (history.pushState) {
        // Сохраняем состояние
        const state = {
            path: url,
            page: pageNum,
            timestamp: new Date().getTime()
        };

        try {
            window.history.pushState(state, '', url);
            console.log('AjaxPagination: URL updated to:', url);
        } catch (e) {
            console.error('AjaxPagination: Failed to update URL:', e);
        }
    }
}

function updateActivePageInPagination(activePageNum) {
    // Обновляем активную страницу во всех блоках пагинации
    document.querySelectorAll('.games-pagination').forEach(pagination => {
        // Обновляем активный класс у элементов списка
        pagination.querySelectorAll('.page-item').forEach(item => {
            item.classList.remove('active');

            const link = item.querySelector('.ajax-pagination-link');
            if (link && link.dataset.page === activePageNum) {
                item.classList.add('active');
            }
        });

        // Обновляем текстовую информацию о странице в этом блоке
        const currentSpan = pagination.querySelector('.page-info [id$="current"]');
        if (currentSpan) {
            currentSpan.textContent = activePageNum;
        }
    });

    // Также обновляем отдельные элементы по ID
    const currentPageSpans = document.querySelectorAll('#games-current, #games-current-top');
    currentPageSpans.forEach(span => {
        span.textContent = activePageNum;
    });

    console.log('AjaxPagination: Active page updated to', activePageNum);
}

function getPageFromUrl(url) {
    // Извлекаем номер страницы из URL
    try {
        const urlObj = new URL(url, window.location.origin);
        const page = urlObj.searchParams.get('page');
        return page || '1';
    } catch (e) {
        console.error('AjaxPagination: Failed to parse URL:', e);

        // Fallback: ищем page= в строке
        const match = url.match(/[?&]page=(\d+)/);
        return match ? match[1] : '1';
    }
}

function showErrorMessage(message) {
    // Показываем сообщение об ошибке
    const gamesContainer = document.querySelector('.games-container');
    if (!gamesContainer) return;

    // Проверяем, есть ли уже сообщение об ошибке
    let errorDiv = document.querySelector('.ajax-pagination-error');

    if (!errorDiv) {
        errorDiv = document.createElement('div');
        errorDiv.className = 'ajax-pagination-error alert alert-danger alert-dismissible fade show mt-3';
        errorDiv.setAttribute('role', 'alert');
        errorDiv.innerHTML = `
            <strong>Error!</strong> <span class="error-message"></span>
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        `;

        // Вставляем перед контейнером с играми
        gamesContainer.parentNode.insertBefore(errorDiv, gamesContainer);
    }

    // Обновляем сообщение
    const errorMessageSpan = errorDiv.querySelector('.error-message');
    if (errorMessageSpan) {
        errorMessageSpan.textContent = message;
    }

    // Показываем
    errorDiv.style.display = 'block';

    // Автоматически скрываем через 5 секунд
    setTimeout(() => {
        if (errorDiv) {
            errorDiv.style.display = 'none';
        }
    }, 5000);
}

// Экспортируем для использования в других модулях
window.AjaxPagination = {
    loadPage: loadGamesPage,
    init: initAjaxPagination
};

console.log('AjaxPagination: Script loaded');