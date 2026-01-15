// games/static/games/js/modules/filters-ui.js

const FilterUI = {
    // Инициализация секций
    initializeSections() {
        console.log('Initializing sections...');
        
        // Восстановление состояний из localStorage
        setTimeout(() => {
            this.restoreSectionStates();
        }, 100);
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
            { btn: '.show-all-game-modes-btn', list: '.game-mode-list' },
            { btn: '.show-all-game-types-btn', list: '.game-type-list' }
        ];

        toggles.forEach(({ btn, list }) => {
            const button = document.querySelector(btn);
            const listElement = document.querySelector(list);

            if (button && listElement) {
                // Проверяем сохраненное состояние
                const isExpanded = this.getShowAllState(list);

                if (isExpanded) {
                    listElement.style.maxHeight = 'none';
                    const showText = button.querySelector('.show-text');
                    const hideText = button.querySelector('.hide-text');
                    if (showText) showText.style.display = 'none';
                    if (hideText) hideText.style.display = 'inline';
                } else {
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
                        this.saveShowAllState(list, false);
                    } else {
                        // Разворачиваем
                        listElement.style.maxHeight = 'none';
                        const showText = button.querySelector('.show-text');
                        const hideText = button.querySelector('.hide-text');
                        if (showText) showText.style.display = 'none';
                        if (hideText) hideText.style.display = 'inline';
                        this.saveShowAllState(list, true);
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

    // Сохранение состояния "Show all"
    saveShowAllState(listSelector, isExpanded) {
        try {
            localStorage.setItem(`show_all_${listSelector.replace('.', '')}`, isExpanded ? 'expanded' : 'collapsed');
        } catch (e) {
            console.warn('Could not save show all state:', e);
        }
    },

    // Получение состояния "Show all"
    getShowAllState(listSelector) {
        try {
            const state = localStorage.getItem(`show_all_${listSelector.replace('.', '')}`);
            return state === 'expanded';
        } catch (e) {
            return false;
        }
    },

    // Настройка переключения секций с сохранением состояния
    setupSectionToggles() {
        console.log('Setting up section toggles...');

        // Сохранение состояния в localStorage
        const saveSectionState = (sectionId, isOpen) => {
            try {
                localStorage.setItem(`filter_section_${sectionId}`, isOpen ? 'open' : 'closed');
            } catch (e) {
                console.warn('Could not save section state:', e);
            }
        };

        const saveSubsectionState = (subsectionId, isOpen) => {
            try {
                localStorage.setItem(`filter_subsection_${subsectionId}`, isOpen ? 'open' : 'closed');
            } catch (e) {
                console.warn('Could not save subsection state:', e);
            }
        };

        const getSectionState = (sectionId) => {
            try {
                return localStorage.getItem(`filter_section_${sectionId}`);
            } catch (e) {
                return null;
            }
        };

        const getSubsectionState = (subsectionId) => {
            try {
                return localStorage.getItem(`filter_subsection_${subsectionId}`);
            } catch (e) {
                return null;
            }
        };

        // Переключение главных секций
        document.querySelectorAll('.toggle-section').forEach(section => {
            section.addEventListener('click', function() {
                const targetId = this.getAttribute('data-target');
                const targetContent = document.getElementById(targetId);
                const icon = this.querySelector('i');

                if (!targetContent || !icon) return;

                if (targetContent.style.display === 'none' || targetContent.style.display === '') {
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

                if (targetContent.style.display === 'none' || targetContent.style.display === '') {
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

                if (targetContent && icon) {
                    if (savedState === 'open') {
                        targetContent.style.display = 'block';
                        icon.classList.remove('bi-chevron-right');
                        icon.classList.add('bi-chevron-down');
                    } else if (savedState === 'closed') {
                        targetContent.style.display = 'none';
                        icon.classList.remove('bi-chevron-down');
                        icon.classList.add('bi-chevron-right');
                    } else {
                        // Если состояние не сохранено, используем умолчания
                        // Главные секции закрыты по умолчанию
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

                if (targetContent && icon) {
                    if (savedState === 'open') {
                        targetContent.style.display = 'block';
                        icon.classList.remove('bi-chevron-right');
                        icon.classList.add('bi-chevron-down');
                    } else if (savedState === 'closed') {
                        targetContent.style.display = 'none';
                        icon.classList.remove('bi-chevron-down');
                        icon.classList.add('bi-chevron-right');
                    } else {
                        // Если состояние не сохранено, подсекции открыты по умолчанию
                        targetContent.style.display = 'block';
                        icon.classList.remove('bi-chevron-right');
                        icon.classList.add('bi-chevron-down');
                    }
                }
            });
        }, 100);
    },

    // Восстановление состояний секций
    restoreSectionStates() {
        console.log('Restoring section states...');

        // Восстанавливаем состояния главных секций
        document.querySelectorAll('.toggle-section').forEach(section => {
            const targetId = section.getAttribute('data-target');
            const savedState = localStorage.getItem(`filter_section_${targetId}`);
            const targetContent = document.getElementById(targetId);
            const icon = section.querySelector('i');

            if (targetContent && icon) {
                if (savedState === 'open') {
                    targetContent.style.display = 'block';
                    icon.classList.remove('bi-chevron-right');
                    icon.classList.add('bi-chevron-down');
                } else if (savedState === 'closed') {
                    targetContent.style.display = 'none';
                    icon.classList.remove('bi-chevron-down');
                    icon.classList.add('bi-chevron-right');
                } else {
                    // По умолчанию главные секции закрыты
                    targetContent.style.display = 'none';
                    icon.classList.remove('bi-chevron-down');
                    icon.classList.add('bi-chevron-right');
                }
            }
        });

        // Восстанавливаем состояния подсекций
        document.querySelectorAll('.toggle-subsection').forEach(subsection => {
            const targetId = subsection.getAttribute('data-target');
            const savedState = localStorage.getItem(`filter_subsection_${targetId}`);
            const targetContent = document.getElementById(targetId);
            const icon = subsection.querySelector('i');

            if (targetContent && icon) {
                if (savedState === 'open') {
                    targetContent.style.display = 'block';
                    icon.classList.remove('bi-chevron-right');
                    icon.classList.add('bi-chevron-down');
                } else if (savedState === 'closed') {
                    targetContent.style.display = 'none';
                    icon.classList.remove('bi-chevron-down');
                    icon.classList.add('bi-chevron-right');
                } else {
                    // По умолчанию подсекции открыты
                    targetContent.style.display = 'block';
                    icon.classList.remove('bi-chevron-right');
                    icon.classList.add('bi-chevron-down');
                }
            }
        });
    },

    // Настройка поиска по фильтрам
    setupSearchInputs() {
        console.log('Setting up search inputs...');

        const searchInputs = [
            'genre-search', 'keyword-search', 'platform-search',
            'theme-search', 'perspective-search', 'game-mode-search',
            'game-type-search'
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

                    // Добавляем отступ для input
                    input.style.paddingRight = '30px';
                }

                // Показывать/скрывать кнопку очистки
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

                updateClearButton();
            }
        });
    },

    // Настройка визуальных эффектов для бейджей
    setupBadgeEffects() {
        console.log('Setting up badge effects...');

        const badgeSelectors = [
            '.active-genre-tag',
            '.active-keyword-tag',
            '.active-platform-tag',
            '.active-theme-tag',
            '.active-perspective-tag',
            '.active-game-mode-tag',
            '.active-game-type-tag'
        ];

        badgeSelectors.forEach(selector => {
            document.querySelectorAll(selector).forEach(badge => {
                badge.addEventListener('mouseenter', function() {
                    this.style.transform = 'scale(1.05)';
                    this.style.transition = 'transform 0.2s ease';
                    this.style.boxShadow = '0 4px 8px rgba(0,0,0,0.2)';
                });

                badge.addEventListener('mouseleave', function() {
                    this.style.transform = 'scale(1)';
                    this.style.boxShadow = 'none';
                });

                // Эффект нажатия
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
            '.game-mode-checkbox',
            '.game-type-checkbox'
        ];

        checkboxSelectors.forEach(selector => {
            document.querySelectorAll(selector).forEach(checkbox => {
                checkbox.addEventListener('change', function() {
                    const label = this.parentElement;
                    if (label) {
                        if (this.checked) {
                            label.style.transform = 'translateX(5px)';
                            label.style.transition = 'transform 0.2s ease';
                        } else {
                            label.style.transform = 'translateX(0)';
                        }

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