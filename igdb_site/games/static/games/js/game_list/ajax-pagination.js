// games/static/games/js/game_list/ajax-pagination.js
/**
 * AJAX Pagination for Game List
 *
 * Полностью заменяет старую клиентскую пагинацию.
 * Использует серверную пагинацию через AJAX-запросы.
 * Совместим с существующими фильтрами.
 * Включает менеджер позиции прокрутки.
 */

const AjaxPagination = {
    config: {
        itemsPerPage: 16,
        currentPage: 1,
        totalPages: 1,
        totalItems: 0,
        containerSelector: '#games-results-container',
        loadingClass: 'loading',
        debug: true
    },

    state: {
        isInitialized: false,
        isLoading: false,
        lastRequestId: 0,
        cache: new Map() // Кэш страниц
    },

    // ===== SCROLL POSITION MANAGER =====
    scrollManager: {
        // Ключ для localStorage
        storageKey: 'games_page_scroll_position',
        // Время жизни сохраненной позиции (5 минут)
        maxAge: 5 * 60 * 1000,
        // Минимальная высота прокрутки для сохранения
        minScrollHeight: 100,

        /**
         * Сохраняет текущую позицию прокрутки
         * Используется ТОЛЬКО при beforeunload (перезагрузка/уход со страницы)
         */
        saveScrollPosition() {
            const scrollY = window.scrollY;

            // Не сохраняем если прокрутка слишком маленькая
            if (scrollY < this.minScrollHeight) {
                this.clearScrollPosition();
                return;
            }

            const pageData = {
                scrollY: scrollY,
                timestamp: Date.now(),
                pageUrl: window.location.pathname + window.location.search,
                pageNumber: AjaxPagination.config.currentPage
            };

            try {
                localStorage.setItem(this.storageKey, JSON.stringify(pageData));
                if (AjaxPagination.config.debug) {
                    console.log(`ScrollManager: Saved scroll position: ${scrollY}px for page ${pageData.pageNumber}`);
                }
            } catch (e) {
                console.warn('ScrollManager: Could not save scroll position to localStorage:', e);
            }
        },

        /**
         * Восстанавливает сохраненную позицию прокрутки
         * Используется ТОЛЬКО при загрузке страницы (после перезагрузки)
         */
        restoreScrollPosition() {
            try {
                const savedData = localStorage.getItem(this.storageKey);
                if (!savedData) return;

                const data = JSON.parse(savedData);

                // Проверяем актуальность данных
                const isExpired = Date.now() - data.timestamp > this.maxAge;
                const isSamePage = data.pageUrl === (window.location.pathname + window.location.search);

                if (isExpired || !isSamePage) {
                    this.clearScrollPosition();
                    return;
                }

                // Ждем пока загрузятся все изображения и контент
                window.addEventListener('load', () => {
                    setTimeout(() => {
                        // Плавная прокрутка к сохраненной позиции
                        window.scrollTo({
                            top: data.scrollY,
                            behavior: 'smooth'
                        });

                        if (AjaxPagination.config.debug) {
                            console.log(`ScrollManager: Restored scroll position to: ${data.scrollY}px`);
                        }

                        // Очищаем после восстановления
                        this.clearScrollPosition();
                    }, 100);
                });

            } catch (e) {
                console.warn('ScrollManager: Could not restore scroll position:', e);
                this.clearScrollPosition();
            }
        },

        /**
         * Очищает сохраненную позицию прокрутки
         */
        clearScrollPosition() {
            try {
                localStorage.removeItem(this.storageKey);
            } catch (e) {
                console.warn('ScrollManager: Could not clear scroll position:', e);
            }
        },

        /**
         * Настраивает сохранение позиции ТОЛЬКО перед перезагрузкой/уходом
         */
        setupBeforeUnload() {
            window.addEventListener('beforeunload', () => {
                this.saveScrollPosition();
            });
        },

        /**
         * Инициализация менеджера прокрутки
         */
        init() {
            this.setupBeforeUnload();
            this.restoreScrollPosition();

            if (AjaxPagination.config.debug) {
                console.log('ScrollManager: Initialized');
            }
        }
    },

    /**
     * Инициализация AJAX пагинации
     */
    init() {
        if (this.state.isInitialized) {
            console.log('AjaxPagination: Already initialized');
            return;
        }

        console.log('AjaxPagination: Initializing...');

        // Загружаем информацию о пагинации из DOM
        this.loadPaginationInfo();

        // Проверяем, есть ли игры
        if (this.config.totalItems === 0) {
            console.log('AjaxPagination: No games found, pagination disabled');
            this.state.isInitialized = true;
            return;
        }

        // Получаем текущую страницу из URL или из скрытого поля
        this.config.currentPage = this.getCurrentPageFromDOM();

        // Инициализируем менеджер прокрутки (ТОЛЬКО для beforeunload)
        this.scrollManager.init();

        // Настраиваем обработчики событий
        this.setupEventListeners();

        this.state.isInitialized = true;

        // Загружаем соседние страницы в фоне
        this.preloadAdjacentPages();

        console.log(`AjaxPagination: Initialized. Page ${this.config.currentPage}/${this.config.totalPages}, Items: ${this.config.totalItems}`);
    },

    /**
     * Загружает информацию о пагинации из DOM
     */
    loadPaginationInfo() {
        const serverTotalElement = document.getElementById('server-total-count');
        const serverTotalPagesElement = document.getElementById('server-total-pages');
        const serverItemsPerPageElement = document.getElementById('server-items-per-page');

        if (serverTotalElement) {
            this.config.totalItems = parseInt(serverTotalElement.value) || 0;
        }

        if (serverTotalPagesElement) {
            this.config.totalPages = parseInt(serverTotalPagesElement.value) || 1;
        }

        if (serverItemsPerPageElement) {
            this.config.itemsPerPage = parseInt(serverItemsPerPageElement.value) || 16;
        }

        // Fallback: пытаемся получить из видимых элементов
        if (this.config.totalItems === 0) {
            const totalElement = document.querySelector('#games-total-all, #games-total-all-top');
            if (totalElement) {
                const totalText = totalElement.textContent.trim();
                const match = totalText.match(/\d+/);
                if (match) {
                    this.config.totalItems = parseInt(match[0]) || 0;
                }
            }
        }

        if (this.config.totalPages === 0 && this.config.totalItems > 0) {
            this.config.totalPages = Math.ceil(this.config.totalItems / this.config.itemsPerPage);
        }

        console.log(`AjaxPagination: Info loaded - Items: ${this.config.totalItems}, Pages: ${this.config.totalPages}, PerPage: ${this.config.itemsPerPage}`);
    },

    /**
     * Получает текущую страницу из DOM
     */
    getCurrentPageFromDOM() {
        // 1. Пробуем из скрытого поля
        const serverCurrentPage = document.getElementById('server-current-page');
        if (serverCurrentPage) {
            const page = parseInt(serverCurrentPage.value);
            if (!isNaN(page) && page >= 1 && page <= this.config.totalPages) {
                return page;
            }
        }

        // 2. Пробуем из URL
        const urlParams = new URLSearchParams(window.location.search);
        const pageFromUrl = urlParams.get('page');
        if (pageFromUrl) {
            const page = parseInt(pageFromUrl);
            if (!isNaN(page) && page >= 1) {
                return Math.min(page, this.config.totalPages);
            }
        }

        // 3. Пробуем из видимого элемента
        const currentElement = document.querySelector('#games-current, #games-current-top');
        if (currentElement) {
            const page = parseInt(currentElement.textContent.trim());
            if (!isNaN(page) && page >= 1) {
                return Math.min(page, this.config.totalPages);
            }
        }

        return 1;
    },

    /**
     * Настройка обработчиков событий
     */
    setupEventListeners() {
        // Удаляем старые обработчики
        this.removeEventListeners();

        // Находим все ссылки пагинации с классом ajax-pagination-link
        const paginationLinks = document.querySelectorAll('.games-pagination .ajax-pagination-link');

        paginationLinks.forEach(link => {
            // Пропускаем disabled ссылки
            if (link.closest('.page-item')?.classList.contains('disabled')) {
                return;
            }

            // Сохраняем оригинальный href для ссылки
            const originalHref = link.getAttribute('href');

            if (!originalHref) return;

            // Парсим URL для получения номера страницы
            try {
                const url = new URL(originalHref, window.location.origin);
                const pageParam = url.searchParams.get('page');

                if (!pageParam) return;

                const pageNumber = parseInt(pageParam);
                if (isNaN(pageNumber)) return;

                // Создаем новый обработчик
                const handler = (e) => {
                    e.preventDefault();
                    e.stopPropagation();

                    // НЕ СОХРАНЯЕМ позицию прокрутки при AJAX-переходе
                    // this.scrollManager.saveScrollPosition(); - УДАЛЕНО

                    this.goToPage(pageNumber);
                };

                // Сохраняем обработчик для возможности удаления
                link._ajaxHandler = handler;
                link.addEventListener('click', handler);
            } catch (e) {
                console.warn('AjaxPagination: Invalid URL in pagination link', originalHref);
            }
        });

        console.log('AjaxPagination: Event listeners setup completed');
    },

    /**
     * Удаляет старые обработчики
     */
    removeEventListeners() {
        const paginationLinks = document.querySelectorAll('.games-pagination .ajax-pagination-link');

        paginationLinks.forEach(link => {
            if (link._ajaxHandler) {
                link.removeEventListener('click', link._ajaxHandler);
                delete link._ajaxHandler;
            }
        });
    },

    /**
     * Переход на указанную страницу
     */
    goToPage(pageNumber) {
        // Валидация
        if (pageNumber < 1 || pageNumber > this.config.totalPages) {
            console.warn(`AjaxPagination: Page ${pageNumber} out of range (1-${this.config.totalPages})`);
            return;
        }

        if (pageNumber === this.config.currentPage) {
            console.log(`AjaxPagination: Already on page ${pageNumber}`);
            return;
        }

        if (this.state.isLoading) {
            console.log('AjaxPagination: Already loading, please wait...');
            return;
        }

        console.log(`AjaxPagination: Loading page ${pageNumber}...`);

        // Показываем индикатор загрузки
        this.showLoadingIndicator();

        // Обновляем URL без перезагрузки страницы
        this.updateURL(pageNumber);

        // Загружаем страницу
        this.loadPage(pageNumber)
            .then(() => {
                this.config.currentPage = pageNumber;
                this.hideLoadingIndicator();
                this.updatePaginationUI();

                // НЕ ПРОКРУЧИВАЕМ наверх - убираем scrollToTop
                // this.scrollToTop(); - УДАЛЕНО

                this.preloadAdjacentPages();
                console.log(`AjaxPagination: Page ${pageNumber} loaded successfully`);
            })
            .catch(error => {
                console.error(`AjaxPagination: Failed to load page ${pageNumber}:`, error);
                this.hideLoadingIndicator();
                alert(`Failed to load page ${pageNumber}. Please try again.`);
            });
    },

    /**
     * Загружает страницу с сервера
     */
    loadPage(pageNumber) {
        return new Promise((resolve, reject) => {
            // Генерируем ID запроса для отмены устаревших
            const requestId = ++this.state.lastRequestId;

            // Проверяем кэш
            if (this.state.cache.has(pageNumber)) {
                const cachedHtml = this.state.cache.get(pageNumber);
                this.updateGamesContainer(cachedHtml, pageNumber);
                resolve();
                return;
            }

            // Получаем текущие параметры URL
            const currentUrl = new URL(window.location.href);
            const params = new URLSearchParams(currentUrl.search);

            // Устанавливаем страницу и AJAX флаг
            params.set('page', pageNumber);
            params.set('_ajax', '1');

            // Строим URL для запроса
            const fetchUrl = `${window.location.pathname}?${params.toString()}`;

            fetch(fetchUrl, {
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                }
            })
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    return response.text();
                })
                .then(html => {
                    // Проверяем, не устарел ли запрос
                    if (requestId !== this.state.lastRequestId) {
                        console.log(`AjaxPagination: Request ${requestId} is stale, ignoring`);
                        return;
                    }

                    // Парсим HTML
                    const parser = new DOMParser();
                    const doc = parser.parseFromString(html, 'text/html');

                    // Извлекаем контейнер с результатами
                    const newContainer = doc.querySelector('#games-results-container');

                    if (!newContainer) {
                        reject(new Error('Games results container not found in response'));
                        return;
                    }

                    const newHtml = newContainer.innerHTML;

                    // Кэшируем
                    this.state.cache.set(pageNumber, newHtml);

                    // Обновляем DOM
                    this.updateGamesContainer(newHtml, pageNumber);

                    resolve();
                })
                .catch(error => {
                    reject(error);
                });
        });
    },

    /**
     * Обновляет контейнер с играми
     */
    updateGamesContainer(html, pageNumber) {
        const container = document.querySelector(this.config.containerSelector);

        if (!container) {
            console.error('AjaxPagination: Games container not found');
            return;
        }

        // СОХРАНЯЕМ текущую позицию прокрутки ПЕРЕД обновлением
        const currentScrollY = window.scrollY;

        // Обновляем содержимое
        container.innerHTML = html;

        // Обновляем скрытые поля с информацией о странице
        this.updateHiddenFields(pageNumber);

        // Перенастраиваем обработчики событий
        this.setupEventListeners();

        // Обновляем pagination UI
        this.updatePaginationUI();

        // ВОССТАНАВЛИВАЕМ позицию прокрутки ПОСЛЕ обновления
        // Используем requestAnimationFrame для гарантии, что DOM обновился
        requestAnimationFrame(() => {
            window.scrollTo({
                top: currentScrollY,
                behavior: 'auto' // Мгновенно, без анимации
            });

            if (this.config.debug) {
                console.log(`AjaxPagination: Restored scroll position to ${currentScrollY}px`);
            }
        });
    },

    /**
     * Обновляет скрытые поля с информацией о странице
     */
    updateHiddenFields(pageNumber) {
        const serverCurrentPage = document.getElementById('server-current-page');
        if (serverCurrentPage) {
            serverCurrentPage.value = pageNumber;
        }

        // Обновляем видимые элементы с номерами страниц
        const currentElements = document.querySelectorAll('#games-current, #games-current-top');
        currentElements.forEach(el => {
            el.textContent = pageNumber;
        });

        // Обновляем start/end индексы
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage + 1;
        const endIndex = Math.min(pageNumber * this.config.itemsPerPage, this.config.totalItems);

        const startElements = document.querySelectorAll('#games-start, #games-start-top');
        startElements.forEach(el => {
            el.textContent = startIndex;
        });

        const endElements = document.querySelectorAll('#games-end, #games-end-top');
        endElements.forEach(el => {
            el.textContent = endIndex;
        });
    },

    /**
     * Обновляет UI пагинации
     */
    updatePaginationUI() {
        const currentPage = this.config.currentPage;
        const totalPages = this.config.totalPages;

        // Обновляем активную страницу в списке номеров
        const pageItems = document.querySelectorAll('.games-pagination .page-item');

        pageItems.forEach(item => {
            const link = item.querySelector('.page-link');
            if (!link) return;

            const href = link.getAttribute('href');
            if (!href) return;

            try {
                const url = new URL(href, window.location.origin);
                const pageParam = url.searchParams.get('page');

                if (pageParam) {
                    const pageNum = parseInt(pageParam);

                    if (pageNum === currentPage) {
                        item.classList.add('active');
                    } else {
                        item.classList.remove('active');
                    }
                }
            } catch (e) {
                // Игнорируем ошибки парсинга
            }
        });

        // Обновляем кнопки Prev/Next
        // Кнопки с aria-label="Previous"
        const prevButtons = document.querySelectorAll('.games-pagination a.page-link[aria-label="Previous"]');

        prevButtons.forEach(btn => {
            const parent = btn.closest('.page-item');
            if (currentPage === 1) {
                parent?.classList.add('disabled');
                btn.setAttribute('aria-disabled', 'true');
                btn.removeAttribute('href');
                btn.classList.remove('ajax-pagination-link');
            } else {
                parent?.classList.remove('disabled');
                btn.setAttribute('href', `?page=${currentPage - 1}`);
                btn.setAttribute('aria-disabled', 'false');
                btn.classList.add('ajax-pagination-link');

                // Обновляем обработчик
                if (btn._ajaxHandler) {
                    btn.removeEventListener('click', btn._ajaxHandler);
                }

                const handler = (e) => {
                    e.preventDefault();
                    // НЕ сохраняем позицию прокрутки
                    this.goToPage(currentPage - 1);
                };

                btn._ajaxHandler = handler;
                btn.addEventListener('click', handler);
            }
        });

        // Кнопки с текстом "Prev" (без aria-label)
        document.querySelectorAll('.games-pagination .page-link').forEach(btn => {
            if (btn.textContent.includes('Prev') && !btn.hasAttribute('aria-label')) {
                const parent = btn.closest('.page-item');
                if (currentPage === 1) {
                    parent?.classList.add('disabled');
                    btn.setAttribute('aria-disabled', 'true');
                    btn.removeAttribute('href');
                    btn.classList.remove('ajax-pagination-link');
                } else {
                    parent?.classList.remove('disabled');
                    btn.setAttribute('href', `?page=${currentPage - 1}`);
                    btn.setAttribute('aria-disabled', 'false');
                    btn.classList.add('ajax-pagination-link');

                    if (btn._ajaxHandler) {
                        btn.removeEventListener('click', btn._ajaxHandler);
                    }

                    const handler = (e) => {
                        e.preventDefault();
                        this.goToPage(currentPage - 1);
                    };

                    btn._ajaxHandler = handler;
                    btn.addEventListener('click', handler);
                }
            }
        });

        // Кнопки Next с aria-label="Next"
        const nextButtons = document.querySelectorAll('.games-pagination a.page-link[aria-label="Next"]');

        nextButtons.forEach(btn => {
            const parent = btn.closest('.page-item');
            if (currentPage === totalPages) {
                parent?.classList.add('disabled');
                btn.setAttribute('aria-disabled', 'true');
                btn.removeAttribute('href');
                btn.classList.remove('ajax-pagination-link');
            } else {
                parent?.classList.remove('disabled');
                btn.setAttribute('href', `?page=${currentPage + 1}`);
                btn.setAttribute('aria-disabled', 'false');
                btn.classList.add('ajax-pagination-link');

                if (btn._ajaxHandler) {
                    btn.removeEventListener('click', btn._ajaxHandler);
                }

                const handler = (e) => {
                    e.preventDefault();
                    this.goToPage(currentPage + 1);
                };

                btn._ajaxHandler = handler;
                btn.addEventListener('click', handler);
            }
        });

        // Кнопки с текстом "Next" (без aria-label)
        document.querySelectorAll('.games-pagination .page-link').forEach(btn => {
            if (btn.textContent.includes('Next') && !btn.hasAttribute('aria-label')) {
                const parent = btn.closest('.page-item');
                if (currentPage === totalPages) {
                    parent?.classList.add('disabled');
                    btn.setAttribute('aria-disabled', 'true');
                    btn.removeAttribute('href');
                    btn.classList.remove('ajax-pagination-link');
                } else {
                    parent?.classList.remove('disabled');
                    btn.setAttribute('href', `?page=${currentPage + 1}`);
                    btn.setAttribute('aria-disabled', 'false');
                    btn.classList.add('ajax-pagination-link');

                    if (btn._ajaxHandler) {
                        btn.removeEventListener('click', btn._ajaxHandler);
                    }

                    const handler = (e) => {
                        e.preventDefault();
                        this.goToPage(currentPage + 1);
                    };

                    btn._ajaxHandler = handler;
                    btn.addEventListener('click', handler);
                }
            }
        });
    },

    /**
     * Показывает индикатор загрузки
     */
    showLoadingIndicator() {
        this.state.isLoading = true;

        const container = document.querySelector(this.config.containerSelector);
        if (container) {
            container.classList.add(this.config.loadingClass);
        }

        // Блокируем кнопки пагинации
        const paginationLinks = document.querySelectorAll('.games-pagination .ajax-pagination-link');
        paginationLinks.forEach(link => {
            link.style.pointerEvents = 'none';
            link.style.opacity = '0.7';
        });
    },

    /**
     * Скрывает индикатор загрузки
     */
    hideLoadingIndicator() {
        this.state.isLoading = false;

        const container = document.querySelector(this.config.containerSelector);
        if (container) {
            container.classList.remove(this.config.loadingClass);
        }

        // Разблокируем кнопки пагинации
        const paginationLinks = document.querySelectorAll('.games-pagination .ajax-pagination-link');
        paginationLinks.forEach(link => {
            link.style.pointerEvents = '';
            link.style.opacity = '';
        });
    },

    /**
     * Обновляет URL без перезагрузки страницы
     */
    updateURL(pageNumber) {
        const url = new URL(window.location.href);

        if (pageNumber === 1) {
            url.searchParams.delete('page');
        } else {
            url.searchParams.set('page', pageNumber);
        }

        window.history.pushState({
            page: pageNumber,
            timestamp: Date.now()
        }, document.title, url.toString());
    },

    /**
     * Предзагружает соседние страницы
     */
    preloadAdjacentPages() {
        const current = this.config.currentPage;
        const total = this.config.totalPages;

        // Страницы для предзагрузки: следующая и предыдущая (если есть)
        const pagesToPreload = [];

        if (current < total && !this.state.cache.has(current + 1)) {
            pagesToPreload.push(current + 1);
        }

        if (current > 1 && !this.state.cache.has(current - 1)) {
            pagesToPreload.push(current - 1);
        }

        if (pagesToPreload.length === 0) return;

        console.log(`AjaxPagination: Preloading pages ${pagesToPreload.join(', ')}...`);

        pagesToPreload.forEach(page => {
            setTimeout(() => {
                if (!this.state.cache.has(page) && !this.state.isLoading) {
                    this.loadPage(page).catch(() => {});
                }
            }, 1000);
        });
    },

    /**
     * Сбрасывает пагинацию при применении фильтров
     */
    reset() {
        console.log('AjaxPagination: Resetting...');

        // Очищаем кэш
        this.state.cache.clear();

        // Сбрасываем на первую страницу
        this.config.currentPage = 1;

        // Обновляем URL
        this.updateURL(1);

        // Перезагружаем информацию о пагинации
        setTimeout(() => {
            this.loadPaginationInfo();
            this.setupEventListeners();
        }, 500);
    },

    /**
     * Полная перезагрузка после применения фильтров
     */
    refresh() {
        console.log('AjaxPagination: Refreshing...');

        // Очищаем кэш
        this.state.cache.clear();

        // Перезагружаем текущую страницу
        this.loadPage(this.config.currentPage)
            .then(() => {
                this.updatePaginationUI();
                console.log('AjaxPagination: Refresh complete');
            })
            .catch(error => {
                console.error('AjaxPagination: Refresh failed:', error);
            });
    }
};

// Инициализация при загрузке DOM
document.addEventListener('DOMContentLoaded', function() {
    // Небольшая задержка для полной загрузки DOM
    setTimeout(() => {
        AjaxPagination.init();
    }, 200);
});

// Обработка кнопок "назад"/"вперед" в браузере
window.addEventListener('popstate', function(event) {
    const urlParams = new URLSearchParams(window.location.search);
    const pageFromUrl = urlParams.get('page');

    if (pageFromUrl) {
        const page = parseInt(pageFromUrl);
        if (!isNaN(page) && AjaxPagination.state.isInitialized) {
            AjaxPagination.goToPage(page);
        }
    } else if (AjaxPagination.state.isInitialized) {
        AjaxPagination.goToPage(1);
    }
});

// Экспорт для использования в других модулях
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AjaxPagination;
} else {
    window.AjaxPagination = AjaxPagination;
}