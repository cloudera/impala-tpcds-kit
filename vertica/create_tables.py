from common import get_ssh_client, vsql, get_parser

CREATE_TABLE_STMTS = [

    ('store_sales', '''
    create table if not exists store_sales
    (
      ss_sold_date_sk           int NOT NULL,
      ss_sold_time_sk           int,
      ss_item_sk                int,
      ss_customer_sk            int,
      ss_cdemo_sk               int,
      ss_hdemo_sk               int,
      ss_addr_sk                int,
      ss_store_sk               int,
      ss_promo_sk               int,
      ss_ticket_number          int,
      ss_quantity               int,
      ss_wholesale_cost         double precision,
      ss_list_price             double precision,
      ss_sales_price            double precision,
      ss_ext_discount_amt       double precision,
      ss_ext_sales_price        double precision,
      ss_ext_wholesale_cost     double precision,
      ss_ext_list_price         double precision,
      ss_ext_tax                double precision,
      ss_coupon_amt             double precision,
      ss_net_paid               double precision,
      ss_net_paid_inc_tax       double precision,
      ss_net_profit             double precision
    )
    partition by ss_sold_date_sk
    ''',),

    ('customer_demographics', '''
    create table if not exists customer_demographics
    (
      cd_demo_sk                int,
      cd_gender                 varchar(16),
      cd_marital_status         varchar(16),
      cd_education_status       varchar(128),
      cd_purchase_estimate      int,
      cd_credit_rating          varchar(16),
      cd_dep_count              int,
      cd_dep_employed_count     int,
      cd_dep_college_count      int
    )
    ''',),

    ('date_dim', '''
    create table if not exists date_dim
    (
      d_date_sk                 int,
      d_date_id                 varchar(64),
      d_date                    date,
      d_month_seq               int,
      d_week_seq                int,
      d_quarter_seq             int,
      d_year                    int,
      d_dow                     int,
      d_moy                     int,
      d_dom                     int,
      d_qoy                     int,
      d_fy_year                 int,
      d_fy_quarter_seq          int,
      d_fy_week_seq             int,
      d_day_name                varchar(16),
      d_quarter_name            varchar(16),
      d_holiday                 varchar(16),
      d_weekend                 varchar(16),
      d_following_holiday       varchar(16),
      d_first_dom               int,
      d_last_dom                int,
      d_same_day_ly             int,
      d_same_day_lq             int,
      d_current_day             varchar(16),
      d_current_week            varchar(16),
      d_current_month           varchar(16),
      d_current_quarter         varchar(16),
      d_current_year            varchar(16)
    )
    ''',),

    ('time_dim', '''
    create table if not exists time_dim
    (
      t_time_sk                 int,
      t_time_id                 varchar(16),
      t_time                    int,
      t_hour                    int,
      t_minute                  int,
      t_second                  int,
      t_am_pm                   varchar(16),
      t_shift                   varchar(16),
      t_sub_shift               varchar(16),
      t_meal_time               varchar(16)
    )
    ''',),

    ('item', '''
    create table if not exists item
    (
      i_item_sk                 int,
      i_item_id                 varchar(16),
      i_rec_start_date          varchar(16),
      i_rec_end_date            varchar(16),
      i_item_desc               varchar(128),
      i_current_price           double precision,
      i_wholesale_cost          double precision,
      i_brand_id                int,
      i_brand                   varchar(128),
      i_class_id                int,
      i_class                   varchar(128),
      i_category_id             int,
      i_category                varchar(128),
      i_manufact_id             int,
      i_manufact                varchar(128),
      i_size                    varchar(128),
      i_formulation             varchar(128),
      i_color                   varchar(128),
      i_units                   varchar(128),
      i_container               varchar(128),
      i_manager_id              int,
      i_product_name            varchar(128)
    )
    ''',),

    ('store', '''
    create table if not exists store
    (
      s_store_sk                int,
      s_store_id                varchar(128),
      s_rec_start_date          date,
      s_rec_end_date            date,
      s_closed_date_sk          int,
      s_store_name              varchar(128),
      s_number_employees        int,
      s_floor_space             int,
      s_hours                   varchar(16),
      s_manager                 varchar(128),
      s_market_id               int,
      s_geography_class         varchar(128),
      s_market_desc             varchar(128),
      s_market_manager          varchar(128),
      s_division_id             int,
      s_division_name           varchar(128),
      s_company_id              int,
      s_company_name            varchar(128),
      s_street_number           varchar(16),
      s_street_name             varchar(64),
      s_street_type             varchar(64),
      s_suite_number            varchar(64),
      s_city                    varchar(128),
      s_county                  varchar(128),
      s_state                   varchar(64),
      s_zip                     varchar(64),
      s_country                 varchar(64),
      s_gmt_offset              double precision,
      s_tax_precentage          double precision
    )
    ''',),

    ('customer', '''
    create table if not exists customer
    (
      c_customer_sk             int,
      c_customer_id             varchar(64),
      c_current_cdemo_sk        int,
      c_current_hdemo_sk        int,
      c_current_addr_sk         int,
      c_first_shipto_date_sk    int,
      c_first_sales_date_sk     int,
      c_salutation              varchar(64),
      c_first_name              varchar(64),
      c_last_name               varchar(64),
      c_preferred_cust_flag     varchar(64),
      c_birth_day               int,
      c_birth_month             int,
      c_birth_year              int,
      c_birth_country           varchar(64),
      c_login                   varchar(128),
      c_email_address           varchar(128),
      c_last_review_date        date
    )
    ''',),

    ('promotion', '''
    create table if not exists promotion
    (
      p_promo_sk                int,
      p_promo_id                varchar(64),
      p_start_date_sk           int,
      p_end_date_sk             int,
      p_item_sk                 int,
      p_cost                    double precision,
      p_response_target         int,
      p_promo_name              varchar(64),
      p_channel_dmail           varchar(128),
      p_channel_email           varchar(128),
      p_channel_catalog         varchar(128),
      p_channel_tv              varchar(128),
      p_channel_radio           varchar(128),
      p_channel_press           varchar(128),
      p_channel_event           varchar(128),
      p_channel_demo            varchar(128),
      p_channel_details         varchar(128),
      p_purpose                 varchar(128),
      p_discount_active         varchar(128)
    )
    ''',),

    ('household_demographics', '''
    create table if not exists household_demographics
    (
      hd_demo_sk                int,
      hd_income_band_sk         int,
      hd_buy_potential          varchar(128),
      hd_dep_count              int,
      hd_vehicle_count          int
    )
    ''',),

    ('customer_address', '''
    create table if not exists customer_address
    (
      ca_address_sk             int,
      ca_address_id             varchar(64),
      ca_street_number          varchar(64),
      ca_street_name            varchar(128),
      ca_street_type            varchar(64),
      ca_suite_number           varchar(64),
      ca_city                   varchar(128),
      ca_county                 varchar(128),
      ca_state                  varchar(64),
      ca_zip                    varchar(64),
      ca_country                varchar(128),
      ca_gmt_offset             int,
      ca_location_type          varchar(128)
    )
    ''',),

    ('inventory', '''
    create table if not exists inventory
    (
      inv_date_sk               int,
      inv_item_sk               int,
      inv_warehouse_sk          int,
      inv_quantity_on_hand      int
    )
    ''',),

]

def create_tables(opts):
    ssh_client = get_ssh_client(opts)
    for tbl_name, tbl_stmt in CREATE_TABLE_STMTS:
        if opts.delete:
            vsql(ssh_client, opts, 'DROP TABLE IF EXISTS %s' % tbl_name)
        vsql(ssh_client, opts, tbl_stmt)

    vsql(ssh_client, opts, '\\d')

if __name__ == '__main__':
    parser = get_parser('Create TPC-SD tables in vertica')
    parser.add_argument('--delete', action='store_true', help='Drop existing tables')
    opts = parser.parse_args()
    create_tables(opts)

