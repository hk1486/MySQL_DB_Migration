from sqlalchemy import Table, select, create_engine
import pandas as pd

def create_table(table_name, dest_metadata, columns, dest_engine):
    new_table = Table(table_name, dest_metadata, *columns)
    new_table.create(dest_engine)
    print(table_name,'테이블 CREATE 완료')


def handle_answer_data(src_table, BATCH_SIZE, src_engine, table_name, dest_engine):
    query = select([src_table]).order_by(src_table.c.no.desc()).limit(BATCH_SIZE)
    batch = src_engine.execute(query).fetchall()
    table_df = pd.DataFrame(batch)

    try:
        table_df.to_sql(table_name, con=dest_engine, if_exists='append', index=False)
    except Exception as e:
        print("DB insert 중 에러 발생",e)

    print(f"{table_name} 테이블 마이그레이션 완료!\n")


def insert_data(src_engine, src_table, table_name, BATCH_SIZE, dest_engine):
    # 총 레코드 개수 확인
    total_records = src_engine.execute(select([src_table])).rowcount
    print(f"Total records in {table_name}: {total_records}")

    offset = 0
    while offset < total_records:
        # offset과 limit를 사용하여 데이터 가져오기
        query = select([src_table]).limit(BATCH_SIZE).offset(offset)
        batch = src_engine.execute(query).fetchall()
        table_df = pd.DataFrame(batch)

        try:
            # DataFrame을 테이블에 삽입
            table_df.to_sql(table_name, con=dest_engine, if_exists='append', index=False)
        except Exception as e:
            print("DB insert 중 에러 발생",e)

        offset += BATCH_SIZE
    print(f"{table_name} 테이블 마이그레이션 완료!\n")