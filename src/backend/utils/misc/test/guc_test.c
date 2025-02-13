#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"

#include "../guc.c"

static int guc_array_compare(const void *a, const void *b)
{
	const char *namea = *(const char **)a;
	const char *nameb = *(const char **)b;

	return guc_name_compare(namea, nameb);
}

static const char *sync_guc_names_array[] =
{
#include "utils/sync_guc_name.h"
};

static const char *unsync_guc_names_array[] =
{
#include "utils/unsync_guc_name.h"
};
static const char *undispatch_guc_names_array[] =
{
	#include "utils/undispatch_guc_name.h"
};
void init(void );

static const struct guc_names_type GucNamesArray_gp[] =
{
	{
		sync_guc_names_array, sizeof(sync_guc_names_array) / sizeof(char *), GUC_GPDB_NEED_SYNC
	},
	{
		unsync_guc_names_array, sizeof(unsync_guc_names_array) / sizeof(char *), GUC_GPDB_NO_SYNC
	},
	{
		undispatch_guc_names_array, sizeof(undispatch_guc_names_array) / sizeof(char *), GUC_GPDB_NO_DISPATCH
	},
	/* End-of-list marker */
	{
		NULL, 0, 0
	}
};
void init()
{
	for (int i = 0; GucNamesArray_gp[i].guc_names_array; i++)
	{
		struct guc_names_type current_type = GucNamesArray_gp[i];
		qsort((void *) (current_type.guc_names_array), current_type.num,
			sizeof(char *), guc_array_compare);
	}
}

static void assert_guc(struct config_generic *conf)
{
	char *res = NULL;
	for (int i = 0; GucNamesArray_gp[i].guc_names_array; i++)
	{
		struct guc_names_type current_type = GucNamesArray_gp[i];
		res = (char *) bsearch((void *) &conf->name,
								(void *) (current_type.guc_names_array),
								current_type.num,
								sizeof(char *),
								guc_array_compare);
		
		if (res)
			break;
	}
	if ( res == NULL)
		printf("GUC: '%s' does not exist in lists.\n", conf->name);
	assert_true(res);
}

static void
test_bool_guc_coverage(void **state)
{
	for (int i = 0; ConfigureNamesBool[i].gen.name; i++)
	{
		struct config_generic *conf = (struct config_generic *)&ConfigureNamesBool[i];
		assert_guc(conf);
	}

}

static void
test_int_guc_coverage(void **state)
{
	for (int i = 0; ConfigureNamesInt[i].gen.name; i++)
	{
		struct config_generic *conf = (struct config_generic *)&ConfigureNamesInt[i];
		assert_guc(conf);
	}

}

static void
test_string_guc_coverage(void **state)
{
	for (int i = 0; ConfigureNamesString[i].gen.name; i++)
	{
		struct config_generic *conf = (struct config_generic *)&ConfigureNamesString[i];
		assert_guc(conf);
	}

}

static void
test_real_guc_coverage(void **state)
{
	for (int i = 0; ConfigureNamesReal[i].gen.name; i++)
	{
		struct config_generic *conf = (struct config_generic *)&ConfigureNamesReal[i];
		assert_guc(conf);
	}

}

static void
test_enum_guc_coverage(void **state)
{
	for (int i = 0; ConfigureNamesEnum[i].gen.name; i++)
	{
		struct config_generic *conf = (struct config_generic *)&ConfigureNamesEnum[i];
		assert_guc(conf);
	}

}

/*
 * a guc name should only be place once.
 */
static void
test_guc_name_list_mutual_exclusion(void **state)
{
	int sync_guc_num = sizeof(sync_guc_names_array) / sizeof(char *);
	int unsync_guc_num = sizeof(unsync_guc_names_array) / sizeof(char *);
	int undispatch_guc_num = sizeof(undispatch_guc_names_array) / sizeof(char *);
	for (int i = 0; i < sync_guc_num; i++)
	{
		char *res = (char *) bsearch((void *) &(sync_guc_names_array[i]),
				(void *) unsync_guc_names_array,
				unsync_guc_num,
				sizeof(char *),
				guc_array_compare);

		if ( res != NULL)
			printf("GUC: '%s' exist in both list(sync, unsync).\n", sync_guc_names_array[i]);

		assert_true(res == NULL);
	}

	for (int i = 0; i < unsync_guc_num; i++)
	{
		char *res = (char *) bsearch((void *) &(unsync_guc_names_array[i]),
				(void *) undispatch_guc_names_array,
				undispatch_guc_num,
				sizeof(char *),
				guc_array_compare);

		if ( res != NULL)
			printf("GUC: '%s' exist in both list(unsync, undispatch).\n", unsync_guc_names_array[i]);

		assert_true(res == NULL);
	}
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);
	init();

	const UnitTest tests[] = {
		unit_test(test_bool_guc_coverage),
		unit_test(test_int_guc_coverage),
		unit_test(test_real_guc_coverage),
		unit_test(test_string_guc_coverage),
		unit_test(test_enum_guc_coverage),
		unit_test(test_guc_name_list_mutual_exclusion)
	};

	return run_tests(tests);
}
