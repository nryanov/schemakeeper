$(window).on('load', function () {
    let api = new SchemaKeeperApi('../..');

    let templates = {};

    $('script[type=x-tmpl-mustache]').each(function (index) {
        let template = $(this).html();
        let templateId = $(this).attr('id');

        Mustache.parse(template);

        templates[templateId] = template;
    });

    api.subjects(function (response) {
        console.log(response);
        let currentPage = 1;
        let subjects = response;
        let pages = pagesCount(subjects);
        let cachedSubjects = {};

        function pagesCount(subjects) {
            return Math.ceil(subjects.length / 10);
        }

        function pagesIndexes(pages) {
            return Array.from(Array(pages).keys()).map(i => {
                let idx = ++i;
                return {
                    'index': idx,
                    'isActive': idx === currentPage
                }
            })
        }

        function getPageN(subjects, page) {
            return subjects.slice((page - 1) * 10, page * 10);
        }

        function searchBySubjectName(subjects, name) {
            return subjects.filter(x => x.includes(name));
        }

        function renderPages(currentPage, pagesCount) {
            let template = Mustache.render(templates['pages-template'], {
                'pages': pagesIndexes(pagesCount),
                'isFirst': currentPage === 1,
                'isLast': currentPage === pagesCount,
                'currentPage': currentPage
            });

            $('#pages-template-target').html(template);
            setupPageListeners();
        }

        function setupPageListeners() {
            $('.page-link.num').click(function () {
                currentPage = $(this).data('page');
                renderSubjectsList(subjects, currentPage);
                renderPages(currentPage, pages);
            });

            $('.page-link.previous').click(function () {
                currentPage = $(this).data('page') - 1;
                renderSubjectsList(subjects, currentPage);
                renderPages(currentPage, pages);
            });

            $('.page-link.next').click(function () {
                currentPage = $(this).data('page') + 1;
                renderSubjectsList(subjects, currentPage);
                renderPages(currentPage, pages);
            });
        }

        function renderSubjectsList(subjects, page) {
            let template = Mustache.render(templates['subjects-template'], {'subjects': getPageN(subjects, page)});
            $('#subjects-template-target').html(template);
            setupSubjectListeners();
        }

        function setupSubjectListeners() {
            $('.subject-btn').click(function () {
                let selectedSubject = $(this).data('subject');
                renderSubjectInfo(selectedSubject);
            })
        }

        function renderSubjectInfo(subject) {
            api.getSubjectMeta(subject, function (response) {
                console.log(response);
                let template = Mustache.render(templates['subject-info-template'], {
                    'subjectInfo': {
                        "subjectName": response.subject,
                        "versions": response.versions,
                        "compatibilityType": response.compatibilityType,
                        "schemaType": response.schemaType,
                        "lastSchema": "schema"
                    }
                });

                $('#subject-info-template-target').html(template);
                setupSubjectInfoListeneres();
            });
        }

        function setupSubjectInfoListeneres() {
            $('#save-btn').click(function () {
                let schema = $('#schema-text').val();
                let subject = $(this).data('subject');
                registerNewSchema(subject, schema);
            });
        }

        function registerNewSchema(subject, schema) {
            api.registerNewSubjectSchema(subject, schema, function (response) {
                console.log(response);
                location.reload(); //todo: temporary solution
            });
        }

        //todo: return error if subject is already exist
        function registerNewSubject() {
            let subjectName = $('#new-subject-name').val();
            let schemaText = $('#new-schema-text').val();
            let compatibilityType = $('#new-subject-compatibility-type').val().toLowerCase();

            api.registerNewSubject(subjectName, schemaText, compatibilityType, function (response) {
                console.log(response);
                cachedSubjects[subjectName] = {"1": schemaText};
                subjects.push(subjectName);
                pages = pagesCount(subjects);

                renderSubjectsList(subjects, currentPage);
                renderPages(currentPage, pages);
            });
        }

        function clearModalInputs() {
            $('#new-subject-name').val('');
            $('#new-schema-text').val('');
            $('#new-subject-compatibility-type').prop('selectedIndex', 0);
        }

        renderSubjectsList(subjects, currentPage);
        renderPages(currentPage, pages);

        $('#search-subject').on('input', function () {
            let input = $(this);
            let filteredSubjects = searchBySubjectName(subjects, input.val());
            currentPage = 1;
            renderSubjectsList(filteredSubjects, currentPage);
            renderPages(currentPage, pagesCount(filteredSubjects));
        });

        $('#modal-close-btn').click(function () {
            clearModalInputs();
        });

        $('#modal-save-btn').click(function () {
            registerNewSubject();
            clearModalInputs();
            $('#subjectModal').modal('toggle');
        });
    });
});
