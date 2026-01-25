// games/static/games/js/game_list/filters-global.js

const FilterGlobal = {
    // Глобальные функции для кнопок Show More/Less
    initializeGlobalFunctions() {
        console.log('Initializing global functions...');
        
        // Platforms
        window.toggleMoreplatforms = (button) => this.toggleMore(button, 'platforms');
        window.toggleLessplatforms = (button) => this.toggleLess(button, 'platforms');
        
        // Genres
        window.toggleMoregenres = (button) => this.toggleMore(button, 'genres');
        window.toggleLessgenres = (button) => this.toggleLess(button, 'genres');
        
        // Keywords
        window.toggleMorekeywords = (button) => this.toggleMore(button, 'keywords');
        window.toggleLesskeywords = (button) => this.toggleLess(button, 'keywords');
        
        // Themes
        window.toggleMorethemes = (button) => this.toggleMore(button, 'themes');
        window.toggleLessthemes = (button) => this.toggleLess(button, 'themes');
        
        // Perspectives
        window.toggleMoreperspectives = (button) => this.toggleMore(button, 'perspectives');
        window.toggleLessperspectives = (button) => this.toggleLess(button, 'perspectives');
    },
    
    toggleMore(button, type) {
        const hiddenSection = document.getElementById(`hidden-${type}-badges`);
        const showLessBtn = button.nextElementSibling;
        
        if (hiddenSection) {
            hiddenSection.style.display = 'block';
            button.style.display = 'none';
            if (showLessBtn) showLessBtn.style.display = 'inline-block';
            
            // Сортируем после изменения
            setTimeout(() => {
                if (window.FilterManager && window.FilterManager.sort) {
                    window.FilterManager.sort.sortFilterLists();
                }
            }, 100);
        }
    },
    
    toggleLess(button, type) {
        const hiddenSection = document.getElementById(`hidden-${type}-badges`);
        const showMoreBtn = button.previousElementSibling;
        
        if (hiddenSection) {
            hiddenSection.style.display = 'none';
            button.style.display = 'none';
            if (showMoreBtn) showMoreBtn.style.display = 'inline-block';
        }
    },
    
    // Инициализация обработчиков формы
    initializeFormHandlers() {
        console.log('Initializing form handlers...');
        
        // Обработчик для кнопки Apply Filters
        const applyButton = document.querySelector('button[type="submit"]');
        if (applyButton) {
            applyButton.addEventListener('click', (e) => {
                this.handleApplyFilters(e);
            });
        }
        
        // Обработчик для кнопки "Show All Games"
        const showAllButton = document.querySelector('a[href*="game_list"]');
        if (showAllButton && !showAllButton.hasAttribute('data-no-scroll')) {
            showAllButton.addEventListener('click', (e) => {
                this.handleShowAllGames(e, showAllButton);
            });
        }
    },
    
    handleApplyFilters(e) {
        // Сохраняем позицию прокрутки перед отправкой формы
        if (window.FilterManager && window.FilterManager.handlers) {
            window.FilterManager.handlers.saveScrollPosition();
        } else {
            this.saveScrollPositionFallback();
        }
        
        // Добавляем параметр _scroll к форме
        const form = document.getElementById('main-search-form');
        if (form) {
            let scrollField = form.querySelector('input[name="_scroll"]');
            if (!scrollField) {
                scrollField = document.createElement('input');
                scrollField.type = 'hidden';
                scrollField.name = '_scroll';
                scrollField.value = '1';
                form.appendChild(scrollField);
            }
        }
    },
    
    handleShowAllGames(e, button) {
        // Сохраняем позицию прокрутки
        if (window.FilterManager && window.FilterManager.handlers) {
            window.FilterManager.handlers.saveScrollPosition();
        } else {
            this.saveScrollPositionFallback();
        }
        
        // Добавляем параметр _scroll к URL
        let href = button.getAttribute('href');
        if (href && !href.includes('_scroll=')) {
            const separator = href.includes('?') ? '&' : '?';
            button.setAttribute('href', href + separator + '_scroll=1');
        }
    },
    
    saveScrollPositionFallback() {
        const scrollPosition = {
            x: window.scrollX || window.pageXOffset,
            y: window.scrollY || window.pageYOffset,
            timestamp: Date.now()
        };
        try {
            sessionStorage.setItem('filterScrollPosition', JSON.stringify(scrollPosition));
        } catch (e) {
            console.warn('Could not save scroll position:', e);
        }
    },
    
    // Обработка параметра _scroll из URL
    handleScrollParameter() {
        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.has('_scroll')) {
            // Удаляем параметр из URL без перезагрузки
            urlParams.delete('_scroll');
            const newUrl = window.location.pathname + 
                          (urlParams.toString() ? '?' + urlParams.toString() : '') + 
                          window.location.hash;
            
            // Заменяем URL без перезагрузки
            window.history.replaceState({}, document.title, newUrl);
        }
    },
    
    // Инициализация всего
    initializeAll() {
        console.log('Initializing FilterGlobal...');
        
        this.initializeGlobalFunctions();
        this.initializeFormHandlers();
        this.handleScrollParameter();
        
        // Добавляем обработчик beforeunload
        window.addEventListener('beforeunload', () => {
            if (window.FilterManager && window.FilterManager.handlers) {
                window.FilterManager.handlers.saveScrollPosition();
            } else {
                this.saveScrollPositionFallback();
            }
        });
        
        console.log('FilterGlobal initialized successfully');
    }
};

// Автоматическая инициализация
document.addEventListener('DOMContentLoaded', () => {
    FilterGlobal.initializeAll();
});

export default FilterGlobal;