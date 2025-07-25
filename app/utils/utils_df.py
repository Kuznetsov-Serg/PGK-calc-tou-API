MAPPING_NAME_COGNOS = {
    "Отчётная дата": "date_rep",
    "Отчетная дата": "date_rep",
    "Код станции ГО": "st_code",
    "Период": "period",
    "Станция выполнения ГО": "st_name",
    "Станция выполнения ГО код": "st_code",
    "Дорога выполнения ГО": "road",
    "Дорога выполнения ГО код": "rw_code",
    "Дорога выполнения ГО Сокр": "rw_short_name",
    "Дорога выполнения ГО Полн": "rw_name",
    "Филиал ГО ID": "org_id",
    "Филиал ГО Сокр": "org_shortname",
    "Филиал ГО Полн": "org_name",
    "Грузоотправитель на станции выполнения ГО ": "shipper",  # Пробел на конце!
    "Грузоотправитель на станции выполнения ГО код": "shipper_cod",
    "Грузоотправитель на станции выполнения ГО": "shipper",
    "Грузополучатель на станции выполнения ГО": "consignee",
    "Грузополучатель на станции выполнения ГО ": "consignee",
    "Грузополучатель на станции выполнения ГО код": "consignee_cod",
    "Id клиента": "id_client",
    "Клиент ID SAP": "client_sap_id",
    "ID Клиента SAP": "client_sap_id",
    "Клиент Наименование": "client",
    "Наименование клиента": "client",
    "Операция тип ВПС": "type_op_vps",
    "Операция тип": "type_op",
    "Тип операции": "type_op",
    "Вагон №": "wagon_num",
    "№ вагона": "wagon_num",
    "Род вагона": "rps_short",
    "РПС код": "rps_cod",
    "РПС Наименование Сокр": "rps_short",
    "РПС Наименование Полн": "rps",
    "№ накладной тек.": "invoice_num_current",
    # "Группа груза, номер тек.": "cargo_group_num",
    "Группа груза Наименование Сокр тек.": "cargo_group_short",
    "Наименование груза тек.": "cargo_current",
    "Груз Наименование Сокр тек.": "cargo_current_short",
    "Груз Наименование Полн тек.": "cargo_current",
    "Груз ЕТСНГ код тек.": "cargo_etsng_cod_current",
    "Код груза ЕТСНГ тек.": "cargo_etsng_cod_current",
    "Дата прибытия тек.": "date_arrival_current",
    "№ накладной след.": "invoice_num_next",
    "Группа груза, номер след.": "cargo_group_num_next",
    "Группа груза Наименование Сокр след.": "cargo_group_next_short",
    "Наименование груза след.": "cargo_name_next",
    "Груз Наименование Сокр след.": "cargo_next_short",
    "Груз Наименование Полн след.": "cargo_next",
    "Груз ЕТСНГ код след.": "cargo_etsng_cod_next",
    "Код груза ЕТСНГ след.": "cargo_etsng_cod_next",
    "Дата приема след.": "date_accept_next",
    "Сдвоенная операция": "double_operation",
    "Простои Факт, ваг-сут": "parking_fact",
    "Факт ваг-сут простоя": "parking_fact",
    "Ваг-сут простоя для сдвоенных": "parking_fact_for_double",
    "Простои Факт ВПС, ваг-сут": "parking_fact_vps",
    "Признак: Используем в ВПС": "use_in_vps",
    "Простои Сдвоенные, ваг-сут": "parking_for_double",
    "Группа груза ГО, номер": "cargo_group_num",
    "Группа груза ГО Наименование Сокр": "cargo_group_go_short",
    "Груз ГО Наименование Сокр": "cargo_go_short",
    "Груз ГО Наименование Полн": "cargo_go",
    "Груз ЕТСНГ ГО код": "cargo_etsng_go",
}

MAPPING_SEASONAL_COEFFICIENT = {
    "Род вагона": "rps_short",
    "Тип операции": "type_operation",
    "id": "type_operation_id",
    "СК01": "Coefficient_01",
    "СК02": "Coefficient_02",
    "СК03": "Coefficient_03",
    "СК04": "Coefficient_04",
    "СК05": "Coefficient_05",
    "СК06": "Coefficient_06",
    "СК07": "Coefficient_07",
    "СК08": "Coefficient_08",
    "СК09": "Coefficient_09",
    "СК10": "Coefficient_10",
    "СК11": "Coefficient_11",
    "СК12": "Coefficient_12",
}

MAPPING_SEASONAL_COEFFICIENT_REVERSE = {val: key for key, val in MAPPING_SEASONAL_COEFFICIENT.items()}
