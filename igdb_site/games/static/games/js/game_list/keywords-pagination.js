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
        maxVisiblePages: 7 // Максимум показываем 7 страниц
    },

    // Инициализация
    init() {
        console.log('Initializing keywords pagination...');

        this.keywordItems = document.querySelectorAll(this.config.itemsSelector);
        this.totalItems = this.keywordItems.length;
        this.totalPages = Math.ceil(this.totalItems / this.config.itemsPerPage);

        if (this.totalItems <= this.config.itemsPerPage) {
            console.log(`Only ${this.totalItems} keywords, no pagination needed.`);
            this.hidePagination();
            // Показываем все элементы
            this.keywordItems.forEach(item => item.style.display = 'block');
            return;
        }

        // Создаем контейнер для страниц
        this.createPageNumbersContainer();

        this.setupPagination();
        this.showPage(1);

        console.log(`Pagination initialized: ${this.totalItems} items, ${this.totalPages} pages`);
    },

    // Создаем контейнер для номеров страниц
    createPageNumbersContainer() {
        const paginationContainer = document.querySelector(this.config.paginationSelector);
        if (!paginationContainer) return;

        // Находим кнопку "предыдущая"
        const prevButton = document.querySelector(this.config.prevButtonId);
        if (!prevButton) return;

        // Создаем контейнер для номеров страниц
        const pageNumbersContainer = document.createElement('div');
        pageNumbersContainer.id = 'keyword-page-numbers';
        pageNumbersContainer.className = 'd-flex flex-wrap gap-1 mx-2 align-items-center';
        pageNumbersContainer.style.minWidth = '150px';

        // Вставляем контейнер после кнопки "предыдущая"
        prevButton.parentNode.insertBefore(pageNumbersContainer, prevButton.nextSibling);
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
        if (!pageNumbersContainer) return;

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
    },

    // Обновить кнопки навигации
    updateNavigationButtons() {
        const prevBtn = document.querySelector(this.config.prevButtonId);
        const nextBtn = document.querySelector(this.config.nextButtonId);

        if (prevBtn) {
            if (this.config.currentPage === 1) {
                prevBtn.classList.add('disabled');
            } else {
                prevBtn.classList.remove('disabled');
            }
        }

        if (nextBtn) {
            if (this.config.currentPage === this.totalPages) {
                nextBtn.classList.add('disabled');
            } else {
                nextBtn.classList.remove('disabled');
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

    // Обновить пагинацию после поиска/фильтрации
    updateAfterSearch() {
        // Получаем видимые элементы после поиска
        const allItems = document.querySelectorAll(this.config.itemsSelector);
        const visibleItems = Array.from(allItems).filter(item =>
            item.style.display !== 'none' &&
            window.getComputedStyle(item).display !== 'none'
        );

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
                const paginationContainer = document.querySelector(this.config.paginationSelector);
                if (paginationContainer) {
                    paginationContainer.style.display = 'flex';
                }

                const paginationNav = document.querySelector('.keyword-pagination');
                if (paginationNav) {
                    paginationNav.style.display = 'block';
                }

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
            const paginationContainer = document.querySelector(this.config.paginationSelector);
            if (paginationContainer) {
                paginationContainer.style.display = 'flex';
            }

            const paginationNav = document.querySelector('.keyword-pagination');
            if (paginationNav) {
                paginationNav.style.display = 'block';
            }

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
        // Пересчитываем элементы
        this.keywordItems = document.querySelectorAll(this.config.itemsSelector);
        this.totalItems = this.keywordItems.length;
        this.totalPages = Math.ceil(this.totalItems / this.config.itemsPerPage);

        if (this.totalItems <= this.config.itemsPerPage) {
            this.hidePagination();
            // Показываем все элементы
            this.keywordItems.forEach(item => item.style.display = 'block');
        } else {
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