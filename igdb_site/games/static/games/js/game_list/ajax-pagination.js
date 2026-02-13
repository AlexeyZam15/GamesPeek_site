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
        debug: true,
        // Флаг для отключения предзагрузки с автоматическим обновлением
        preloadWithAutoUpdate: false
    },

    state: {
        isInitialized: false,
        isLoading: false,
        lastRequestId: 0,
        cache: new Map(), // Кэш страниц
        // Флаг для отслеживания активного перехода
        isNavigating: false
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

        // Настраиваем MutationObserver для предотвращения мигания кнопок
        this.setupPaginationObserver();

        this.state.isInitialized = true;

        // Загружаем соседние страницы в фоне БЕЗ ОБНОВЛЕНИЯ DOM
        this.preloadAdjacentPagesSilent();

        console.log(`AjaxPagination: Initialized. Page ${this.config.currentPage}/${this.config.totalPages}, Items: ${this.config.totalItems}`);
    },

    /**
     * Настраивает MutationObserver для предотвращения мигания кнопок
     */
    setupPaginationObserver() {
        // Наблюдаем за изменениями в контейнере пагинации
        const paginationContainers = document.querySelectorAll('.games-pagination');

        if (paginationContainers.length === 0) return;

        paginationContainers.forEach(container => {
            // Сохраняем ссылки на кнопки до изменений
            let buttonStates = new Map();

            const observer = new MutationObserver((mutations) => {
                // Проверяем, была ли это навигация или просто обновление
                if (!this.state.isNavigating) {
                    // Если это не навигация, сохраняем состояние кнопок
                    this.preserveButtonStyles(container);
                }

                // Быстрое восстановление обработчиков
                setTimeout(() => {
                    this.setupEventListeners();
                    this.restoreActivePageStyles(container);
                }, 0);
            });

            observer.observe(container, {
                childList: true,
                subtree: true,
                attributes: true,
                attributeFilter: ['class', 'href']
            });
        });
    },

    /**
     * Сохраняет и восстанавливает стили кнопок пагинации
     */
    preserveButtonStyles(container) {
        const activeButton = container.querySelector('.page-item.active .page-link');
        if (activeButton) {
            const styles = {
                backgroundColor: activeButton.style.backgroundColor,
                background: activeButton.style.background,
                color: activeButton.style.color,
                boxShadow: activeButton.style.boxShadow
            };
            container._preservedActiveStyles = styles;
        }
    },

    /**
     * Восстанавливает стили активной страницы
     */
    restoreActivePageStyles(container) {
        const activeButton = container.querySelector('.page-item.active .page-link');
        if (activeButton && container._preservedActiveStyles) {
            const styles = container._preservedActiveStyles;
            activeButton.style.backgroundColor = styles.backgroundColor;
            activeButton.style.background = styles.background;
            activeButton.style.color = styles.color;
            activeButton.style.boxShadow = styles.boxShadow;
        }
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
     * Настройка обработчиков событий - используем делегирование для предотвращения мигания
     */
    setupEventListeners() {
        // Удаляем старые обработчики
        this.removeEventListeners();

        // ИСПОЛЬЗУЕМ ДЕЛЕГИРОВАНИЕ - вешаем один обработчик на body
        // Это предотвращает мигание, т.к. обработчик не теряется при обновлении DOM
        const bodyHandler = (e) => {
            // Проверяем, кликнули ли по ссылке пагинации
            const link = e.target.closest('.games-pagination .ajax-pagination-link');

            if (!link) return;

            // Пропускаем disabled ссылки
            if (link.closest('.page-item')?.classList.contains('disabled')) {
                e.preventDefault();
                return;
            }

            const href = link.getAttribute('href');
            if (!href) return;

            try {
                const url = new URL(href, window.location.origin);
                const pageParam = url.searchParams.get('page');

                if (!pageParam) return;

                const pageNumber = parseInt(pageParam);
                if (isNaN(pageNumber)) return;

                e.preventDefault();
                e.stopPropagation();

                // Устанавливаем флаг навигации
                this.state.isNavigating = true;

                this.goToPage(pageNumber);

                // Сбрасываем флаг через небольшую задержку
                setTimeout(() => {
                    this.state.isNavigating = false;
                }, 300);

            } catch (ex) {
                console.warn('AjaxPagination: Invalid URL in pagination link', href);
            }
        };

        // Сохраняем обработчик для возможности удаления
        this._bodyHandler = bodyHandler.bind(this);
        document.body.addEventListener('click', this._bodyHandler);

        console.log('AjaxPagination: Event listeners setup completed (delegation)');
    },

    /**
     * Удаляет старые обработчики
     */
    removeEventListeners() {
        if (this._bodyHandler) {
            document.body.removeEventListener('click', this._bodyHandler);
            delete this._bodyHandler;
        }

        // Удаляем старые прямые обработчики, если они ещё есть
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

        // Устанавливаем флаг навигации
        this.state.isNavigating = true;

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

                // НЕ ПРОКРУЧИВАЕМ наверх

                // Предзагружаем соседние страницы в фоне БЕЗ ОБНОВЛЕНИЯ
                this.preloadAdjacentPagesSilent();

                console.log(`AjaxPagination: Page ${pageNumber} loaded successfully`);

                // Сбрасываем флаг навигации
                setTimeout(() => {
                    this.state.isNavigating = false;
                }, 300);
            })
            .catch(error => {
                console.error(`AjaxPagination: Failed to load page ${pageNumber}:`, error);
                this.hideLoadingIndicator();
                this.state.isNavigating = false;
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

        // Временно отключаем анимацию для предотвращения мигания
        const gamesContainer = document.querySelector('.games-container');
        if (gamesContainer) {
            gamesContainer.style.transition = 'none';
        }

        // Обновляем содержимое
        container.innerHTML = html;

        // Обновляем скрытые поля с информацией о странице
        this.updateHiddenFields(pageNumber);

        // Перенастраиваем обработчики событий (но делегирование уже работает)

        // Обновляем pagination UI
        this.updatePaginationUI();

        // Восстанавливаем анимацию
        setTimeout(() => {
            if (gamesContainer) {
                gamesContainer.style.transition = '';
            }
        }, 100);

        // ВОССТАНАВЛИВАЕМ позицию прокрутки ПОСЛЕ обновления
        requestAnimationFrame(() => {
            window.scrollTo({
                top: currentScrollY,
                behavior: 'auto'
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

        // Обновляем кнопки Prev/Next (просто меняем href, обработчики через делегирование)
        this.updatePrevNextButtons();
    },

    /**
     * Обновляет кнопки Prev/Next
     */
    updatePrevNextButtons() {
        const currentPage = this.config.currentPage;
        const totalPages = this.config.totalPages;

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
            }
        });

        // Кнопки с текстом "Prev"
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
            }
        });

        // Кнопки с текстом "Next"
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

        // Блокируем кнопки пагинации через CSS класс, не удаляя обработчики
        const paginationContainers = document.querySelectorAll('.games-pagination');
        paginationContainers.forEach(container => {
            container.classList.add('pagination-loading');
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
        const paginationContainers = document.querySelectorAll('.games-pagination');
        paginationContainers.forEach(container => {
            container.classList.remove('pagination-loading');
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
     * Предзагружает соседние страницы в фоне БЕЗ ОБНОВЛЕНИЯ DOM
     * Только кэширует HTML, не обновляет контейнер
     */
    preloadAdjacentPagesSilent() {
        const current = this.config.currentPage;
        const total = this.config.totalPages;

        // Страницы для предзагрузки: следующая и предыдущая
        const pagesToPreload = [];

        if (current < total && !this.state.cache.has(current + 1)) {
            pagesToPreload.push(current + 1);
        }

        if (current > 1 && !this.state.cache.has(current - 1)) {
            pagesToPreload.push(current - 1);
        }

        if (pagesToPreload.length === 0) return;

        console.log(`AjaxPagination: Silently preloading pages ${pagesToPreload.join(', ')}...`);

        pagesToPreload.forEach(page => {
            setTimeout(() => {
                if (!this.state.cache.has(page) && !this.state.isLoading) {
                    this.preloadPageSilent(page).catch(() => {});
                }
            }, 1000);
        });
    },

    /**
     * Загружает страницу в кэш БЕЗ ОБНОВЛЕНИЯ DOM
     */
    preloadPageSilent(pageNumber) {
        return new Promise((resolve, reject) => {
            if (this.state.cache.has(pageNumber)) {
                resolve();
                return;
            }

            const currentUrl = new URL(window.location.href);
            const params = new URLSearchParams(currentUrl.search);
            params.set('page', pageNumber);
            params.set('_ajax', '1');

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
                    const parser = new DOMParser();
                    const doc = parser.parseFromString(html, 'text/html');
                    const newContainer = doc.querySelector('#games-results-container');

                    if (newContainer) {
                        const newHtml = newContainer.innerHTML;
                        // Сохраняем в кэш, НО НЕ ОБНОВЛЯЕМ DOM
                        this.state.cache.set(pageNumber, newHtml);
                        console.log(`AjaxPagination: Silently preloaded page ${pageNumber} to cache`);
                    }
                    resolve();
                })
                .catch(error => {
                    console.error(`AjaxPagination: Failed to preload page ${pageNumber}:`, error);
                    reject(error);
                });
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
            AjaxPagination.state.isNavigating = true;
            AjaxPagination.goToPage(page);
            setTimeout(() => {
                AjaxPagination.state.isNavigating = false;
            }, 300);
        }
    } else if (AjaxPagination.state.isInitialized) {
        AjaxPagination.state.isNavigating = true;
        AjaxPagination.goToPage(1);
        setTimeout(() => {
            AjaxPagination.state.isNavigating = false;
        }, 300);
    }
});

// Экспорт для использования в других модулях
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AjaxPagination;
} else {
    window.AjaxPagination = AjaxPagination;
}