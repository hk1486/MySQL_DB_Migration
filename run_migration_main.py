from sqlalchemy import create_engine, MetaData, Table, Column, inspect
from urllib.parse import quote_plus
import time

import migrate_module

# Source DB 개발팀 운영서버 177
src_host = "1234.1234"
src_user = "test"
src_password = quote_plus("여기에 비밀번호 입력") # DB비번에 골뱅이를 인식해주기 위해 quote_plus 사용
src_databases = ['test_DB'] # 여기에 DB를 추가

# Destination DB
dest_host = "1234.1234"
dest_user = "test"
dest_password = quote_plus("여기에 비밀번호 입력") # DB비번에 골뱅이를 인식해주기 위해 quote_plus 사용
dest_databases = ['test_DB'] # 여기에 DB를 추가

if __name__ == '__main__':

    # DB 개수만큼 for문 돌기
    for src_db, dest_db in zip(src_databases, dest_databases):
        # 마이그레이션 시작 시간 기록
        start_time = time.time()

        # DB 연결
        src_engine = create_engine(f"mysql+pymysql://{src_user}:{src_password}@{src_host}/{src_db}")
        dest_engine = create_engine(f"mysql+pymysql://{dest_user}:{dest_password}@{dest_host}/{dest_db}")

        # DB inspect 연결
        src_inspector = inspect(src_engine)
        dest_inspector = inspect(dest_engine)

        # DB 메타데이터 불러오기
        src_metadata = MetaData(bind=src_engine)
        dest_metadata = MetaData(bind=dest_engine)

        # 대상 데이터베이스의 테이블 이름 목록 가져오기
        dest_table_names = dest_inspector.get_table_names()

        # 한번에 가져올 레코드 수 지정
        BATCH_SIZE = 20000

        # 소스DB에 있는 테이블들 전부 for문으로 돌기
        for table_name in src_inspector.get_table_names():

            print(table_name)

            # 테이블 별 마이그레이션 시작 시간 기록
            table_start_time = time.time()

            # 소스 테이블 로드
            src_table = Table(table_name, src_metadata, autoload_with=src_engine)

            # 컬럼들을 복사하여 새로운 컬럼 리스트를 생성
            columns = []
            for column in src_table.columns:
                columns.append(Column(column.name, column.type, primary_key=column.primary_key))

            # 테이블이 대상 데이터베이스에 존재하지 않는 경우만 테이블 생성
            if table_name not in dest_table_names:
                migrate_module.create_table(table_name, dest_metadata, columns, dest_engine)
            else:
                print(table_name, '테이블이 이미 존재합니다.')

            # 대상 테이블 (이미 생성되어 있다고 가정)
            dest_table = Table(table_name, dest_metadata, autoload_with=dest_engine)

            # AnswerTable은 너무 커서 최신 20000개의 레코드만 가져오기
            if table_name in ['SyntaxAnswers', 'SyntaxAnswerTemps', 'SyntaxLearnAnswers']:
                migrate_module.handle_answer_data(src_table, BATCH_SIZE, src_engine, table_name, dest_engine)

            # 로그테이블은 레코드 불러오지 않고, 테이블만 만들기
            # 나머지 테이블들은 레코드 불러와서 INSERT
            elif table_name not in ['LogTable']:
                migrate_module.insert_data(src_engine, src_table, table_name, BATCH_SIZE, dest_engine)

            # 테이블 별 마이그레이션 종료 시간 기록 및 걸린 시간 계산
            table_end_time = time.time()
            table_elapsed_time = table_end_time - table_start_time
            # 테이블 별 걸린 시간 출력
            print(f"{table_name} 테이블 마이그레이션에 걸린 시간: {table_elapsed_time:.2f} 초\n")

        # 마이그레이션 종료 시간 기록 및 걸린 시간 계산
        end_time = time.time()
        elapsed_time = end_time - start_time
        # 걸린 시간 출력
        print(f"{src_db} 마이그레이션에 걸린 시간: {elapsed_time:.2f} 초")