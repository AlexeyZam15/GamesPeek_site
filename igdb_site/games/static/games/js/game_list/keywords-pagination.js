// games/static/games/js/game_list/keywords-pagination.js

const KeywordsPagination = {
    // Конфигурация
    config: {
        itemsPerPage: 30,
        currentPage: 1,
        itemsSelector: '.keyword-item',
        containerSelector: '.keyword-grid',
        paginationSelector: '#keyword-pagination',
        prevButtonId: '#keyword-prev',
        nextButtonId: '#keyword-next',
        startElementId: '#keyword-start',
        endElementId: '#keyword-end',
        currentElementId: '#keyword-current',
        totalElementId: '#keyword-total',
        maxVisiblePages: 7
    },

    // Состояние
    keywordItems: null,
    totalItems: 0,
    totalPages: 0,

    // Инициализация - ИСПРАВЛЕННАЯ ВЕРСИЯ
    init() {
        console.log('Initializing keywords pagination...');

        // Сначала убедимся, что DOM полностью загружен
        this.keywordItems = document.querySelectorAll(this.config.itemsSelector);
        this.totalItems = this.keywordItems.length;
        this.totalPages = Math.ceil(this.totalItems / this.config.itemsPerPage);

        console.log(`Found ${this.totalItems} keyword items, ${this.totalPages} pages`);

        // ПРОВЕРКА 1: Если нет элементов вообще
        if (this.totalItems === 0) {
            console.log('No keyword items found, skipping pagination.');
            return;
        }

        // ПРОВЕРКА 2: Если меньше или равно itemsPerPage, пагинация не нужна
        if (this.totalItems <= this.config.itemsPerPage) {
            console.log(`Only ${this.totalItems} keywords, no pagination needed.`);
            // Показываем все элементы
            this.keywordItems.forEach(item => item.style.display = 'block');

            // Скрываем пагинацию, если контейнер существует
            this.hidePagination();
            return;
        }

        // ПОИСК КОНТЕЙНЕРА ПАГИНАЦИИ (сначала ищем по ID, потом по классу)
        let paginationContainer = document.querySelector(this.config.paginationSelector);

        if (!paginationContainer) {
            // Если контейнер с ID не найден, ищем по классу
            const keywordPagination = document.querySelector('.keyword-pagination');
            if (keywordPagination) {
                console.log('Found keyword-pagination container without ID, adding ID to it');
                // Добавляем ID к существующему элементу
                keywordPagination.id = this.config.paginationSelector.substring(1); // Убираем #
                paginationContainer = keywordPagination;
            }
        }

        if (!paginationContainer) {
            console.warn(`Pagination container not found in DOM. Skipping initialization.`);
            return;
        }

        // ОБНОВЛЯЕМ HTML ЭЛЕМЕНТЫ С ID (если они не имеют ID)
        this.ensureHtmlElementsHaveIds();

        // Создаем контейнер для номеров страниц (если его еще нет)
        this.createPageNumbersContainer();

        this.setupPagination();
        this.showPage(1);

        console.log(`Keywords pagination initialized: ${this.totalItems} items, ${this.totalPages} pages`);
    },

    // НОВЫЙ МЕТОД: Гарантируем, что HTML элементы имеют правильные ID
    ensureHtmlElementsHaveIds() {
        // Проверяем и добавляем ID к элементам пагинации, если их нет
        const elementsToCheck = [
            { selector: '#keyword-prev', defaultId: 'keyword-prev' },
            { selector: '#keyword-next', defaultId: 'keyword-next' },
            { selector: '#keyword-start', defaultId: 'keyword-start' },
            { selector: '#keyword-end', defaultId: 'keyword-end' },
            { selector: '#keyword-current', defaultId: 'keyword-current' },
            { selector: '#keyword-total', defaultId: 'keyword-total' }
        ];

        elementsToCheck.forEach(({ selector, defaultId }) => {
            let element = document.querySelector(selector);

            // Если элемент не найден по ID, ищем по другим способам
            if (!element) {
                // Ищем по текстовому содержимому для информационных элементов
                if (defaultId.includes('start') || defaultId.includes('end') ||
                    defaultId.includes('current') || defaultId.includes('total')) {

                    const allSpans = document.querySelectorAll('.keyword-pagination span');
                    for (let span of allSpans) {
                        if (span.id === defaultId) {
                            element = span;
                            break;
                        }
                        // Если у span нет ID, но он находится в правильном контексте
                        if (!span.id && span.closest('.keyword-pagination')) {
                            const parentText = span.parentElement.textContent;
                            if ((defaultId === 'keyword-start' && parentText.includes('Page')) ||
                                (defaultId === 'keyword-current' && parentText.includes('Page')) ||
                                (defaultId === 'keyword-total-pages' && parentText.includes('of'))) {
                                span.id = defaultId;
                                element = span;
                                break;
                            }
                        }
                    }
                }

                // Для кнопок ищем по классу и позиции
                if (!element && (defaultId.includes('prev') || defaultId.includes('next'))) {
                    const buttons = document.querySelectorAll('.keyword-pagination button');
                    if (buttons.length >= 2) {
                        if (defaultId === 'keyword-prev') {
                            buttons[0].id = defaultId;
                            element = buttons[0];
                        } else if (defaultId === 'keyword-next') {
                            buttons[buttons.length - 1].id = defaultId;
                            element = buttons[buttons.length - 1];
                        }
                    }
                }
            }

            // Если элемент все еще не найден, создаем его
            if (!element) {
                console.warn(`Element ${defaultId} not found, but continuing anyway`);
            }
        });
    },

    // Создаем контейнер для номеров страниц
    createPageNumbersContainer() {
        const paginationContainer = document.querySelector(this.config.paginationSelector);
        if (!paginationContainer) {
            console.error('Pagination container not found:', this.config.paginationSelector);
            return;
        }

        // Находим кнопку "предыдущая"
        const prevButton = document.querySelector(this.config.prevButtonId);
        if (!prevButton) {
            console.error('Previous button not found:', this.config.prevButtonId);
            return;
        }

        // Проверяем, не существует ли уже контейнер
        if (document.getElementById('keyword-page-numbers')) {
            console.log('Page numbers container already exists');
            return;
        }

        // Создаем контейнер для номеров страниц
        const pageNumbersContainer = document.createElement('div');
        pageNumbersContainer.id = 'keyword-page-numbers';
        pageNumbersContainer.className = 'd-flex flex-wrap gap-1 mx-2 align-items-center';
        pageNumbersContainer.style.minWidth = '150px';

        // Вставляем контейнер после кнопки "предыдущая"
        prevButton.parentNode.insertBefore(pageNumbersContainer, prevButton.nextSibling);
        console.log('Created page numbers container');
    },

    // Настройка пагинации
    setupPagination() {
        this.updatePageNumbers();
        this.setupNavigationButtons();
        this.updatePageInfo();
    },

    // Обновить номера страниц
    updatePageNumbers() {
        const pageNumbersContainer = document.getElementById('keyword-page-numbers');
        if (!pageNumbersContainer) {
            console.error('Page numbers container not found');
            return;
        }

        // Очищаем контейнер
        pageNumbersContainer.innerHTML = '';

        // Определяем диапазон страниц для показа
        const { startPage, endPage } = this.getVisiblePageRange(this.config.currentPage);

        // Добавляем первую страницу если она не в диапазоне
        if (startPage > 1) {
            this.createPageNumberButton(pageNumbersContainer, 1);

            // Добавляем многоточие если нужно
            if (startPage > 2) {
                this.createEllipsis(pageNumbersContainer);
            }
        }

        // Добавляем страницы в диапазоне
        for (let i = startPage; i <= endPage; i++) {
            this.createPageNumberButton(pageNumbersContainer, i);
        }

        // Добавляем последнюю страницу если она не в диапазоне
        if (endPage < this.totalPages) {
            // Добавляем многоточие если нужно
            if (endPage < this.totalPages - 1) {
                this.createEllipsis(pageNumbersContainer);
            }

            this.createPageNumberButton(pageNumbersContainer, this.totalPages);
        }

        console.log(`Updated page numbers: ${startPage} to ${endPage}`);
    },

    // Создать кнопку номера страницы
    createPageNumberButton(container, pageNumber) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'btn btn-sm page-number-btn';

        if (pageNumber === this.config.currentPage) {
            button.classList.add('btn-primary');
        } else {
            button.classList.add('btn-outline-primary');
        }

        button.textContent = pageNumber;
        button.dataset.page = pageNumber;

        button.addEventListener('click', (e) => {
            e.preventDefault();
            this.showPage(pageNumber);
        });

        container.appendChild(button);
        return button;
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
        // Для небольшого количества страниц показываем все
        if (this.totalPages <= this.config.maxVisiblePages) {
            return { startPage: 1, endPage: this.totalPages };
        }

        let startPage = currentPage - Math.floor(this.config.maxVisiblePages / 2);
        let endPage = currentPage + Math.floor(this.config.maxVisiblePages / 2);

        // Корректируем если вышли за границы
        if (startPage < 1) {
            startPage = 1;
            endPage = this.config.maxVisiblePages;
        }

        if (endPage > this.totalPages) {
            endPage = this.totalPages;
            startPage = endPage - this.config.maxVisiblePages + 1;
        }

        return { startPage, endPage };
    },

    // Настройка кнопок навигации
    setupNavigationButtons() {
        const prevBtn = document.querySelector(this.config.prevButtonId);
        const nextBtn = document.querySelector(this.config.nextButtonId);

        if (prevBtn) {
            // Удаляем старые обработчики
            const newPrevBtn = prevBtn.cloneNode(true);
            prevBtn.parentNode.replaceChild(newPrevBtn, prevBtn);

            newPrevBtn.addEventListener('click', (e) => {
                e.preventDefault();
                if (this.config.currentPage > 1) {
                    this.showPage(this.config.currentPage - 1);
                }
            });
        }

        if (nextBtn) {
            // Удаляем старые обработчики
            const newNextBtn = nextBtn.cloneNode(true);
            nextBtn.parentNode.replaceChild(newNextBtn, nextBtn);

            newNextBtn.addEventListener('click', (e) => {
                e.preventDefault();
                if (this.config.currentPage < this.totalPages) {
                    this.showPage(this.config.currentPage + 1);
                }
            });
        }
    },

    // Показать определенную страницу
    showPage(pageNumber) {
        if (pageNumber < 1 || pageNumber > this.totalPages) return;

        console.log(`Showing keywords page ${pageNumber}`);
        this.config.currentPage = pageNumber;

        // Показываем/скрываем элементы
        const startIndex = (pageNumber - 1) * this.config.itemsPerPage;
        const endIndex = Math.min(startIndex + this.config.itemsPerPage, this.totalItems);

        this.keywordItems.forEach((item, index) => {
            if (index >= startIndex && index < endIndex) {
                item.style.display = 'block';
            } else {
                item.style.display = 'none';
            }
        });

        // Обновляем номера страниц
        this.updatePageNumbers();

        // Обновляем информацию
        this.updatePageInfo();

        // Обновляем кнопки навигации
        this.updateNavigationButtons();

        // Обновляем сортировку для текущей страницы
        setTimeout(() => {
            if (window.FilterManager && window.FilterManager.sort) {
                window.FilterManager.sort.quickSortFilterList(
                    '.keyword-grid',
                    '.keyword-item',
                    '.keyword-checkbox'
                );
            }
        }, 100);

        console.log(`Showing keywords page ${pageNumber} (items ${startIndex + 1}-${endIndex})`);
    },

    // Обновить информацию о странице
    updatePageInfo() {
        const startIndex = (this.config.currentPage - 1) * this.config.itemsPerPage + 1;
        const endIndex = Math.min(this.config.currentPage * this.config.itemsPerPage, this.totalItems);

        const startElement = document.querySelector(this.config.startElementId);
        const endElement = document.querySelector(this.config.endElementId);
        const currentElement = document.querySelector(this.config.currentElementId);
        const totalElement = document.querySelector(this.config.totalElementId);

        if (startElement) startElement.textContent = startIndex;
        if (endElement) endElement.textContent = endIndex;
        if (currentElement) currentElement.textContent = this.config.currentPage;
        if (totalElement) totalElement.textContent = this.totalItems;

        console.log(`Page info: ${startIndex}-${endIndex} of ${this.totalItems}`);
    },

    // Обновить кнопки навигации
    updateNavigationButtons() {
        const prevBtn = document.querySelector(this.config.prevButtonId);
        const nextBtn = document.querySelector(this.config.nextButtonId);

        if (prevBtn) {
            if (this.config.currentPage === 1) {
                prevBtn.classList.add('disabled');
                prevBtn.disabled = true;
            } else {
                prevBtn.classList.remove('disabled');
                prevBtn.disabled = false;
            }
        }

        if (nextBtn) {
            if (this.config.currentPage === this.totalPages) {
                nextBtn.classList.add('disabled');
                nextBtn.disabled = true;
            } else {
                nextBtn.classList.remove('disabled');
                nextBtn.disabled = false;
            }
        }
    },

    // Скрыть пагинацию
    hidePagination() {
        const paginationContainer = document.querySelector(this.config.paginationSelector);
        if (paginationContainer) {
            paginationContainer.style.display = 'none';
        }

        const paginationNav = document.querySelector('.keyword-pagination');
        if (paginationNav) {
            paginationNav.style.display = 'none';
        }
    },

    // Показать пагинацию
    showPagination() {
        const paginationContainer = document.querySelector(this.config.paginationSelector);
        if (paginationContainer) {
            paginationContainer.style.display = 'flex';
        }

        const paginationNav = document.querySelector('.keyword-pagination');
        if (paginationNav) {
            paginationNav.style.display = 'block';
        }
    },

    // Обновить пагинацию после поиска/фильтрации
    updateAfterSearch() {
        // Получаем видимые элементы после поиска
        const allItems = document.querySelectorAll(this.config.itemsSelector);
        const visibleItems = Array.from(allItems).filter(item =>
            item.style.display !== 'none' &&
            window.getComputedStyle(item).display !== 'none'
        );

        console.log(`After search: ${visibleItems.length} visible items`);

        if (visibleItems.length <= this.config.itemsPerPage) {
            // Показываем все видимые элементы
            allItems.forEach(item => {
                if (visibleItems.includes(item)) {
                    item.style.display = 'block';
                } else {
                    item.style.display = 'none';
                }
            });

            // Обновляем данные
            this.keywordItems = visibleItems;
            this.totalItems = visibleItems.length;
            this.totalPages = Math.ceil(this.totalItems / this.config.itemsPerPage);

            if (this.totalItems <= this.config.itemsPerPage) {
                this.hidePagination();
            } else {
                // Показываем пагинацию
                this.showPagination();

                // Перестраиваем пагинацию
                this.setupPagination();
                this.showPage(1);
            }
        } else {
            // Обновляем данные
            this.keywordItems = allItems;
            this.totalItems = visibleItems.length;
            this.totalPages = Math.ceil(this.totalItems / this.config.itemsPerPage);

            // Показываем пагинацию
            this.showPagination();

            // Перестраиваем пагинацию
            this.setupPagination();
            this.showPage(1);
        }
    },

    // Сбросить к первой странице
    resetToFirstPage() {
        this.showPage(1);
    },

    // Принудительно обновить после изменений DOM
    forceUpdate() {
        console.log('Force updating keywords pagination...');

        // Пересчитываем элементы
        this.keywordItems = document.querySelectorAll(this.config.itemsSelector);
        this.totalItems = this.keywordItems.length;
        this.totalPages = Math.ceil(this.totalItems / this.config.itemsPerPage);

        console.log(`Force update: ${this.totalItems} items, ${this.totalPages} pages`);

        // УДАЛЯЕМ ЛИШНИЕ КОНТЕЙНЕРЫ ПАГИНАЦИИ
        const allPaginationContainers = document.querySelectorAll('.keyword-pagination');
        if (allPaginationContainers.length > 1) {
            console.log(`Found ${allPaginationContainers.length} pagination containers, keeping only the one with ID`);
            // Оставляем только контейнер с ID, остальные удаляем
            allPaginationContainers.forEach((container, index) => {
                if (!container.id || container.id !== 'keyword-pagination') {
                    console.log(`Removing extra pagination container #${index}`);
                    container.remove();
                }
            });
        }

        if (this.totalItems <= this.config.itemsPerPage) {
            this.hidePagination();
            // Показываем все элементы
            this.keywordItems.forEach(item => item.style.display = 'block');
        } else {
            // Используем существующий контейнер
            if (!document.querySelector(this.config.paginationSelector)) {
                console.log('No pagination container found, skipping');
                return;
            }

            // Создаем контейнер если его нет
            if (!document.getElementById('keyword-page-numbers')) {
                this.createPageNumbersContainer();
            }

            this.setupPagination();
            this.showPage(1);
        }
    }
};

// Экспорт для использования в других модулях
export default KeywordsPagination;