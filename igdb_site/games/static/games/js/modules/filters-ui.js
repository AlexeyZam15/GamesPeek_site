// games/static/games/js/modules/filters-ui.js

const FilterUI = {
    // Инициализация секций
    initializeSections() {
        console.log('Initializing sections...');
        
        // Главные секции скрыты по умолчанию
        document.querySelectorAll('.toggle-section').forEach(section => {
            const targetId = section.getAttribute('data-target');
            const targetContent = document.getElementById(targetId);

            if (targetContent) {
                targetContent.style.display = 'none';
                const icon = section.querySelector('i');
                if (icon) {
                    icon.classList.remove('bi-chevron-down');
                    icon.classList.add('bi-chevron-right');
                }
            }
        });

        // Подсекции раскрыты по умолчанию
        document.querySelectorAll('.toggle-subsection').forEach(subsection => {
            const targetId = subsection.getAttribute('data-target');
            const targetContent = document.getElementById(targetId);
            const icon = subsection.querySelector('i');

            if (targetContent && icon) {
                targetContent.style.display = 'block';
                icon.classList.remove('bi-chevron-right');
                icon.classList.add('bi-chevron-down');
            }
        });
    },

    // Настройка кнопок "Show all"
    setupShowAllToggles() {
        console.log('Setting up show all toggles...');

        const toggles = [
            { btn: '.show-all-genres-btn', list: '.genre-list' },
            { btn: '.show-all-keywords-btn', list: '.keyword-list' },
            { btn: '.show-all-platforms-btn', list: '.platform-list' },
            { btn: '.show-all-themes-btn', list: '.theme-list' },
            { btn: '.show-all-perspectives-btn', list: '.perspective-list' },
            { btn: '.show-all-game-modes-btn', list: '.game-mode-list' }
        ];

        toggles.forEach(({ btn, list }) => {
            const button = document.querySelector(btn);
            const listElement = document.querySelector(list);

            if (button && listElement) {
                // Проверяем текущее состояние
                const isExpanded = listElement.style.maxHeight === 'none' ||
                                  listElement.style.maxHeight === '';

                if (!isExpanded) {
                    listElement.style.maxHeight = '200px';
                }

                button.addEventListener('click', (e) => {
                    e.preventDefault();

                    const currentIsExpanded = listElement.style.maxHeight === 'none' ||
                                            listElement.style.maxHeight === '';

                    if (currentIsExpanded) {
                        // Сворачиваем
                        listElement.style.maxHeight = '200px';
                        const showText = button.querySelector('.show-text');
                        const hideText = button.querySelector('.hide-text');
                        if (showText) showText.style.display = 'inline';
                        if (hideText) hideText.style.display = 'none';
                    } else {
                        // Разворачиваем
                        listElement.style.maxHeight = 'none';
                        const showText = button.querySelector('.show-text');
                        const hideText = button.querySelector('.hide-text');
                        if (showText) showText.style.display = 'none';
                        if (hideText) hideText.style.display = 'inline';
                    }

                    // Даем время на изменение DOM перед сортировкой
                    setTimeout(() => {
                        if (window.FilterManager && window.FilterManager.sort) {
                            window.FilterManager.sort.sortFilterLists();
                        }
                    }, 100);
                });
            }
        });
    },

    // Настройка переключения секций
    setupSectionToggles() {
        console.log('Setting up section toggles...');

        // Сохранение состояния в localStorage
        function saveSectionState(sectionId, isOpen) {
            try {
                localStorage.setItem(`filter_section_${sectionId}`, isOpen ? 'open' : 'closed');
            } catch (e) {
                console.warn('Could not save section state:', e);
            }
        }

        function saveSubsectionState(subsectionId, isOpen) {
            try {
                localStorage.setItem(`filter_subsection_${subsectionId}`, isOpen ? 'open' : 'closed');
            } catch (e) {
                console.warn('Could not save subsection state:', e);
            }
        }

        function getSectionState(sectionId) {
            try {
                return localStorage.getItem(`filter_section_${sectionId}`);
            } catch (e) {
                return null;
            }
        }

        function getSubsectionState(subsectionId) {
            try {
                return localStorage.getItem(`filter_subsection_${subsectionId}`);
            } catch (e) {
                return null;
            }
        }

        // Переключение главных секций
        document.querySelectorAll('.toggle-section').forEach(section => {
            section.addEventListener('click', function() {
                const targetId = this.getAttribute('data-target');
                const targetContent = document.getElementById(targetId);
                const icon = this.querySelector('i');

                if (!targetContent || !icon) return;

                if (targetContent.style.display === 'none') {
                    targetContent.style.display = 'block';
                    icon.classList.remove('bi-chevron-right');
                    icon.classList.add('bi-chevron-down');
                    saveSectionState(targetId, true);
                } else {
                    targetContent.style.display = 'none';
                    icon.classList.remove('bi-chevron-down');
                    icon.classList.add('bi-chevron-right');
                    saveSectionState(targetId, false);
                }

                // Триггер сортировки после переключения секции
                setTimeout(() => {
                    if (window.FilterManager && window.FilterManager.sort) {
                        window.FilterManager.sort.sortFilterLists();
                    }
                }, 50);
            });
        });

        // Переключение подсекций
        document.querySelectorAll('.toggle-subsection').forEach(subsection => {
            subsection.addEventListener('click', function() {
                const targetId = this.getAttribute('data-target');
                const targetContent = document.getElementById(targetId);
                const icon = this.querySelector('i');

                if (!targetContent || !icon) return;

                if (targetContent.style.display === 'none') {
                    targetContent.style.display = 'block';
                    icon.classList.remove('bi-chevron-right');
                    icon.classList.add('bi-chevron-down');
                    saveSubsectionState(targetId, true);
                } else {
                    targetContent.style.display = 'none';
                    icon.classList.remove('bi-chevron-down');
                    icon.classList.add('bi-chevron-right');
                    saveSubsectionState(targetId, false);
                }

                // Триггер сортировки после переключения подсекции
                setTimeout(() => {
                    if (window.FilterManager && window.FilterManager.sort) {
                        window.FilterManager.sort.sortFilterLists();
                    }
                }, 50);
            });
        });

        // Восстановление сохраненных состояний
        setTimeout(() => {
            // Главные секции
            document.querySelectorAll('.toggle-section').forEach(section => {
                const targetId = section.getAttribute('data-target');
                const savedState = getSectionState(targetId);
                const targetContent = document.getElementById(targetId);
                const icon = section.querySelector('i');

                if (targetContent && icon && savedState) {
                    if (savedState === 'open') {
                        targetContent.style.display = 'block';
                        icon.classList.remove('bi-chevron-right');
                        icon.classList.add('bi-chevron-down');
                    } else {
                        targetContent.style.display = 'none';
                        icon.classList.remove('bi-chevron-down');
                        icon.classList.add('bi-chevron-right');
                    }
                }
            });

            // Подсекции
            document.querySelectorAll('.toggle-subsection').forEach(subsection => {
                const targetId = subsection.getAttribute('data-target');
                const savedState = getSubsectionState(targetId);
                const targetContent = document.getElementById(targetId);
                const icon = subsection.querySelector('i');

                if (targetContent && icon && savedState) {
                    if (savedState === 'open') {
                        targetContent.style.display = 'block';
                        icon.classList.remove('bi-chevron-right');
                        icon.classList.add('bi-chevron-down');
                    } else {
                        targetContent.style.display = 'none';
                        icon.classList.remove('bi-chevron-down');
                        icon.classList.add('bi-chevron-right');
                    }
                }
            });
        }, 100);
    },

    // Настройка поиска по фильтрам (совместно с FilterSearch)
    setupSearchInputs() {
        console.log('Setting up search inputs...');

        // Добавляем обработчики очистки поиска
        const searchInputs = [
            'genre-search', 'keyword-search', 'platform-search',
            'theme-search', 'perspective-search', 'game-mode-search'
        ];

        searchInputs.forEach(inputId => {
            const input = document.getElementById(inputId);
            if (input) {
                // Создаем кнопку очистки
                const clearBtn = document.createElement('button');
                clearBtn.type = 'button';
                clearBtn.className = 'btn btn-sm btn-link position-absolute end-0 top-50 translate-middle-y';
                clearBtn.innerHTML = '<i class="bi bi-x-circle"></i>';
                clearBtn.style.display = 'none';
                clearBtn.style.zIndex = '5';
                clearBtn.style.right = '10px';
                clearBtn.style.background = 'transparent';
                clearBtn.style.border = 'none';
                clearBtn.style.cursor = 'pointer';

                // Находим родительский элемент
                let wrapper = input.parentElement;
                if (wrapper) {
                    wrapper.style.position = 'relative';
                    wrapper.appendChild(clearBtn);

                    // Добавляем отступ для input, чтобы текст не перекрывался с кнопкой
                    input.style.paddingRight = '30px';
                }

                // Показывать/скрывать кнопку очистки при вводе текста
                const updateClearButton = () => {
                    clearBtn.style.display = input.value ? 'block' : 'none';
                };

                input.addEventListener('input', updateClearButton);
                input.addEventListener('change', updateClearButton);

                // Очистка при нажатии на кнопку
                clearBtn.addEventListener('click', () => {
                    input.value = '';
                    input.focus();
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    clearBtn.style.display = 'none';

                    // Сортируем после очистки
                    setTimeout(() => {
                        if (window.FilterManager && window.FilterManager.sort) {
                            window.FilterManager.sort.sortFilterLists();
                        }
                    }, 100);
                });

                // Скрыть кнопку при потере фокуса если поле пустое
                input.addEventListener('blur', () => {
                    setTimeout(() => {
                        if (!input.value) {
                            clearBtn.style.display = 'none';
                        }
                    }, 200);
                });

                // Показать кнопку при фокусе если есть текст
                input.addEventListener('focus', () => {
                    if (input.value) {
                        clearBtn.style.display = 'block';
                    }
                });

                // Инициализируем состояние кнопки
                updateClearButton();
            }
        });
    },

    // Настройка визуальных эффектов для бейджей
    setupBadgeEffects() {
        console.log('Setting up badge effects...');

        // Добавляем hover эффекты для активных бейджей
        const badgeSelectors = [
            '.active-genre-tag',
            '.active-keyword-tag',
            '.active-platform-tag',
            '.active-theme-tag',
            '.active-perspective-tag',
            '.active-game-mode-tag'
        ];

        badgeSelectors.forEach(selector => {
            document.querySelectorAll(selector).forEach(badge => {
                // Добавляем анимацию при наведении
                badge.addEventListener('mouseenter', function() {
                    this.style.transform = 'scale(1.05)';
                    this.style.transition = 'transform 0.2s ease';
                    this.style.boxShadow = '0 4px 8px rgba(0,0,0,0.2)';
                });

                badge.addEventListener('mouseleave', function() {
                    this.style.transform = 'scale(1)';
                    this.style.boxShadow = 'none';
                });

                // Добавляем эффект нажатия
                badge.addEventListener('mousedown', function() {
                    this.style.transform = 'scale(0.95)';
                });

                badge.addEventListener('mouseup', function() {
                    this.style.transform = 'scale(1.05)';
                });
            });
        });
    },

    // Анимация переключения чекбоксов
    setupCheckboxAnimations() {
        console.log('Setting up checkbox animations...');

        const checkboxSelectors = [
            '.genre-checkbox',
            '.keyword-checkbox',
            '.platform-checkbox',
            '.theme-checkbox',
            '.perspective-checkbox',
            '.game-mode-checkbox'
        ];

        checkboxSelectors.forEach(selector => {
            document.querySelectorAll(selector).forEach(checkbox => {
                // Добавляем анимацию при изменении состояния
                checkbox.addEventListener('change', function() {
                    const label = this.parentElement;
                    if (label) {
                        if (this.checked) {
                            label.style.transform = 'translateX(5px)';
                            label.style.transition = 'transform 0.2s ease';
                        } else {
                            label.style.transform = 'translateX(0)';
                        }

                        // Возвращаем к исходному состоянию через 200мс
                        setTimeout(() => {
                            label.style.transform = 'translateX(0)';
                        }, 200);
                    }
                });
            });
        });
    },

    // Инициализация всех UI компонентов
    initializeAllUI() {
        console.log('Initializing all UI components...');

        try {
            this.initializeSections();
            this.setupShowAllToggles();
            this.setupSectionToggles();
            this.setupSearchInputs();
            this.setupBadgeEffects();
            this.setupCheckboxAnimations();

            console.log('All UI components initialized successfully');
        } catch (error) {
            console.error('Error initializing UI components:', error);
        }
    }
};

export default FilterUI;