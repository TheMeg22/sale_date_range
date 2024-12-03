from flask import Flask, render_template, request, send_file
import psycopg2
import pandas as pd
from datetime import datetime

# Database connection parameters
host = 'host'
port = port
database = 'database name'
user = 'user name'
password = 'password'
app = Flask(__name__)
# Function to CA SARL DATA
def ca_sarl_data(start_date, end_date):
    connection = None
    try:
        # Establish database connection
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode="require"
        )
        
        # SQL query to fetch voucher references and sale order details
        query_1 = """
        SELECT
    av.reference AS voucher_reference_df1,
    av.number AS voucher_number_df1,
    sol.product_id,
    p.name_template AS product_name_df1,
    p.default_code AS product_reference_df1,
    sol.price_unit AS price_unit_df1,
    sol.discount,
    sol.product_uom_qty AS quantity_sol_df1,
    sol.price_unit * (sol.discount / 100) AS discount_amount_df1,
    sol.price_unit - (sol.price_unit * (sol.discount / 100)) AS discounted_price_df1,
    so.date_order AS date_order_df1,
    so.amount_tax AS amount_ht_df1 ,  -- Adding the amount_ht field from sale_order
    so.amount_untaxed AS amount_untaxed_df1,  -- Adding the amount_untaxed field from sale_order
    so.amount_total AS amount_total_df1,  -- Adding the amount_total field from sale_order
    COALESCE(so.amount_timbre, 0) AS amount_timbre_df1,
    av.amount AS amount_df1,  -- Adding the amount field from account_voucher
    av.state AS state_df1,  -- Adding the state field from account_voucher
    rp.name AS partner_name_df1,  -- partner_name will be NULL if no matching partner exists
    sw.name AS warehouse_name_df1  -- warehouse_name will be NULL if no matching warehouse exists
    FROM
    account_voucher av
    JOIN
    sale_order so ON av.reference = so.name  -- Link voucher with sale order using the reference field
    JOIN
    sale_order_line sol ON so.id = sol.order_id
    JOIN
    product_product p ON sol.product_id = p.id
    LEFT JOIN
    res_partner rp ON so.partner_id = rp.id  -- LEFT JOIN to get the partner's name, even if it's NULL
    LEFT JOIN
    stock_warehouse sw ON so.warehouse_id = sw.id  -- LEFT JOIN to get the warehouse name, even if it's NULL
    WHERE
    so.date_order >= %s AND so.date_order <= %s
    AND (rp.name IS NULL OR rp.name != 'divers')
    ORDER BY
    so.date_order
    ;

        """

        # Execute query_1 using pandas
        df_1 = pd.read_sql_query(query_1, connection, params=(start_date, end_date))

        # SQL query to fetch invoice details
        query_2 = """
        SELECT
    ail.id AS invoice_line_id_df2,
    ail.product_id AS product_id_df2,
    pp.default_code AS product_code_df2,
    pp.name_template AS product_name_df2,
    ail.quantity AS quantity_df2,
    ail.price_unit AS unit_price_df2,
    ail.discount AS discount_percentage_df2,  -- Added discount percentage
    (ail.price_unit * (ail.discount / 100)) AS discount_amount_df2,  -- Calculating discount amount
    (ail.price_unit - (ail.price_unit * (ail.discount / 100))) AS discount_price_df2,  -- Discounted price after applying discount
    ai.id AS invoice_id_df2,
    ai.date_invoice AS invoice_date_df2,
    COALESCE(ai.amount_timbre, 0) AS amount_timbre_df2,
    ai.number AS facture_df2,
    ai.origin AS invoice_origin_df2,
    ai.state AS invoice_state_df2,
    ai.amount_untaxed AS amount_untaxed_invoice_df2,
    ai.amount_total AS amount_total_invoice_df2,
    rp.name AS partner_name_df2,
    rp.company_id AS company_id_df2,
    c.name AS company_name_df2,
    apm.name AS payment_mode_df2,  -- Payment mode from account_invoice
    ou.name AS operating_unit_name_df2,  -- Operating unit name
    v.number AS voucher_number_df2,  -- Voucher number from account_voucher
    apm_v.name AS voucher_payment_mode_df2  -- Payment mode from account_voucher
FROM
    account_invoice_line ail
JOIN
    account_invoice ai ON ai.id = ail.invoice_id
JOIN
    res_partner rp ON rp.id = ai.partner_id
LEFT JOIN
    res_company c ON c.id = rp.company_id
LEFT JOIN
    account_payment_mode apm ON apm.id = ai.payment_mode_id  -- Payment mode for the invoice
JOIN
    product_product pp ON pp.id = ail.product_id
LEFT JOIN
    operating_unit ou ON ou.id = ai.operating_unit_id  -- Join to get operating unit name
LEFT JOIN
    voucher_invoice_rel av_rel ON av_rel.invoice_id = ai.id  -- Join to link invoice to voucher
LEFT JOIN
    account_voucher v ON v.id = av_rel.voucher_id  -- Join to get voucher details
LEFT JOIN
    account_payment_mode apm_v ON apm_v.id = v.mode_id  -- Join to get payment mode for the voucher
WHERE
    ai.date_invoice BETWEEN %s AND %s
    AND (rp.name IS NULL OR rp.name != 'divers');

        """

        # Execute query_2 using pandas
        df_2 = pd.read_sql_query(query_2, connection, params=(start_date, end_date))

        # Save both DataFrames to separate Excel sheets
        with pd.ExcelWriter('vouchers_and_invoices_data_9.xlsx', engine='openpyxl') as writer:
            df_1.to_excel(writer, index=False, sheet_name='Sale Order Data TICKET')
            df_2.to_excel(writer, index=False, sheet_name='Invoice Data FACTURES')

        print("Data has been successfully saved to 'vouchers_and_invoices_data_9.xlsx'.")

        # Create the report dataframe
        df3 = pd.DataFrame()
        
        # Create the reference column
        df3['Document'] = pd.concat([df_1['voucher_reference_df1'], df_2['invoice_origin_df2']], ignore_index=True)

        # Add document type based on 'Document' column
        df3['Type Doc'] = df3['Document'].apply(lambda x: 'TICKET DE RETOUR' if x.startswith('TK/RET') else
                                                ('FACTURE' if x.startswith('BC/') else
                                                 ('TICKET' if x.startswith('TK') else
                                                  ('FACTURE DE RETOUR' if x.startswith('RET/') or x.startswith('BAV/') else ''))))

        # Add other columns from both DataFrames
        df3['Document status'] = pd.concat([df_1['state_df1'], df_2['invoice_state_df2']], ignore_index=True)
        df3['Amount Total'] = pd.concat([df_1['amount_df1'], df_2['amount_total_invoice_df2']], ignore_index=True)
        df3['Payment Proof'] = pd.concat([df_1['voucher_number_df1'], df_2['voucher_number_df2']], ignore_index=True)
        df3['Product REF'] = pd.concat([df_1['product_reference_df1'], df_2['product_code_df2']], ignore_index=True)
        df3['Product Name'] = pd.concat([df_1['product_name_df1'], df_2['product_name_df2']], ignore_index=True)
        df3['Product Quantity'] = pd.concat([df_1['quantity_sol_df1'], df_2['quantity_df2']], ignore_index=True)
        df3['Price Unit'] = pd.concat([df_1['price_unit_df1'], df_2['unit_price_df2']], ignore_index=True)
        df3['Discount Ammount'] = pd.concat([df_1['discount_amount_df1'], df_2['discount_amount_df2']], ignore_index=True)
        df3['Discounted Price'] = pd.concat([df_1['discounted_price_df1'], df_2['discount_price_df2']], ignore_index=True)
        df3['Client Name'] = pd.concat([df_1['partner_name_df1'], df_2['partner_name_df2']], ignore_index=True)
        df3['STORE'] = pd.concat([df_1['warehouse_name_df1'], df_2['operating_unit_name_df2']], ignore_index=True)
        df3['Timbre'] = pd.concat([df_1['amount_timbre_df1'], df_2['amount_timbre_df2']], ignore_index=True)
        
        # Create date column and sort the dataframe
        df3['Date'] = pd.concat([df_1['date_order_df1'], df_2['invoice_date_df2']], ignore_index=True)
        df3['Date'] = pd.to_datetime(df3['Date'])
        df3 = df3.sort_values(by='Date', ascending=False).reset_index(drop=True)

        # Add HT and TVA columns
        df3['Prix total HT'] = (df3['Discounted Price'] * df3['Product Quantity']) / 1.19
        df3['TVA'] = df3['Prix total HT'] * 0.19  # Assuming VAT is 19%
        df3['Prix total TTC'] = df3['Prix total HT'] + df3['TVA']

        # Multiply specific columns by -1 for returns
        df3.loc[df3['Type Doc'].isin(['FACTURE DE RETOUR', 'TICKET DE RETOUR']),
                ['Discounted Price', 'Price Unit', 'Product Quantity', 'Prix total HT', 'TVA', 'Prix total TTC']] *= -1

        # Filter out open documents
        df3 = df3[df3['Document status'] != 'open']
        df3 = df3[df3['Document status'] != 'draft']
        df3 = df3[df3['Payment Proof'].notna() & (df3['Payment Proof'] != '')]


        # Save final report to Excel
        output_file = 'CA_SARL_Repport.xlsx'
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df3.to_excel(writer, index=False, sheet_name='CA SARL')
        
        return output_file
        print("Final report has been saved to '999.xlsx'.")

    except Exception as e:
        print(f"Error while connecting to the database: {e}")
    finally:
        # Safely close the connection
        if connection:
            connection.close()



def ca_bs_bsf_data(start_date, end_date):
    connection = None
    try:
        # Establish database connection
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode="require"
        )



    except Exception as e:
        print(f"Error while connecting to the database: {e}")
    finally:
        # Safely close the connection
        if connection:
            connection.close()


# Call the function with desired date range
#fetch_data('2024-01-01', '2024-11-01')
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Get the start and end dates from the form
        start_date = request.form['start_date']
        end_date = request.form['end_date']

        # Validate the date format
        try:
            # Convert the input dates to datetime objects to check if the format is correct
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

            if start_date_obj > end_date_obj:
                return "Error: Start date cannot be later than end date.", 400
            
            # Adjust the end date to the last moment of the day (23:59:59)
            end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)

            # Fetch data from the database based on the entered date range
            excel_file = ca_sarl_data(start_date_obj, end_date_obj)
            

            if excel_file:
                # Return the file for download
                return send_file(excel_file,
                                 download_name="CA_SARL_RP.xlsx",
                                 as_attachment=True)
            else:
                return "Error: Unable to fetch data from the database.", 500
        except ValueError:
            return "Error: Invalid date format. Please use YYYY-MM-DD.", 400

    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
