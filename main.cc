#include <assert.h>
#include <stdio.h>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <supersonic/supersonic.h>
#include <supersonic/base/infrastructure/init.h>
#include "gtest/gtest.h"
using namespace std;

using supersonic::Expression;
using supersonic::RandInt32;
using supersonic::Table;
using supersonic::TupleSchema;
using supersonic::NULLABLE;
using supersonic::BOOL;
using supersonic::INT32;
using supersonic::DOUBLE;
using supersonic::STRING;
using supersonic::DATE;

using supersonic::HeapBufferAllocator;
using supersonic::TableRowWriter;
using supersonic::Operation;
using supersonic::FailureOrOwned;
using supersonic::Cursor;
using supersonic::ResultView;
using supersonic::SucceedOrDie;
using supersonic::View;
using supersonic::NamedAttribute;
using supersonic::Plus;
using supersonic::AttributeAt;
using supersonic::BoundExpressionTree;
using supersonic::Attribute;
using supersonic::EvaluationResult;
using supersonic::NOT_NULLABLE;
using supersonic::AggregationSpecification;
using supersonic::SUM;
using supersonic::SingleSourceProjector;
using supersonic::CompoundSingleSourceProjector;
using supersonic::ProjectNamedAttribute;
using supersonic::ProjectAttributeAt;
using supersonic::GroupAggregate;
using supersonic::Arena;
using supersonic::MAX;
using supersonic::MIN;
using supersonic::SortOrder;
using supersonic::ASCENDING;
using supersonic::BoundExpression;
using supersonic::Block;
using supersonic::IsOdd;
using supersonic::Filter;
using supersonic::Generate;

using supersonic::ConstInt32;
using supersonic::ConstString;
using supersonic::ConstBool;
using supersonic::CompoundExpression;

using supersonic::TableSink;
using supersonic::rowcount_t;
using supersonic::ParseStringNulling;
using supersonic::CompoundMultiSourceProjector;
using supersonic::ProjectNamedAttributeAs;

using supersonic::HashJoinOperation;
using supersonic::INNER;
using supersonic::UNIQUE;

static Table *createAuthorTable() {
  TupleSchema author_schema;
  author_schema.add_attribute(Attribute("author_id", INT32, NOT_NULLABLE));
  author_schema.add_attribute(Attribute("name", STRING, NOT_NULLABLE));
  author_schema.add_attribute(Attribute("nobel", BOOL, NOT_NULLABLE));
  return new Table(author_schema, HeapBufferAllocator::Get());
}

// use tuple.
static int addAuthor(Table *table, int id, string name, bool nobel) {
  // insert into tab values (id, name, nobel)
  scoped_ptr<CompoundExpression> tuple(new CompoundExpression());
  tuple->AddAs("author_id", ConstInt32(id));
  tuple->AddAs("name", ConstString(StringPiece(name)));
  tuple->AddAs("nobel", ConstBool(nobel));

  scoped_ptr<Operation> compute(Compute(tuple.release(), Generate(1)));
  scoped_ptr<Cursor> cursor(SucceedOrDie(compute->CreateCursor()));
  scoped_ptr<ResultView> result(new ResultView(cursor->Next(-1)));
  if (result->has_data()) {
    // TableSink like TableRowWriter.
    TableSink sink(table);
    SucceedOrDie(sink.Write(result->view()));
  }
  return result->view().column(0).typed_data<INT32>()[0];
}

static Table *createBookTable() {
  TupleSchema book_schema;
  book_schema.add_attribute(Attribute("book_id", INT32, NOT_NULLABLE));
  book_schema.add_attribute(Attribute("author_id_ref", INT32, NOT_NULLABLE));
  book_schema.add_attribute(Attribute("title", STRING, NOT_NULLABLE));
  book_schema.add_attribute(Attribute("date_published", DATE, NULLABLE));
  return new Table(book_schema, HeapBufferAllocator::Get());
}

static int addBook(Table *book, int book_id, int author_id_ref, StringPiece title, StringPiece date) {
  scoped_ptr<const Expression> date_or_null(ParseStringNulling(DATE, ConstString(date)));
  scoped_ptr<const Expression> author_id_ref_or_null(
      If(Less(ConstInt32(author_id_ref), ConstInt32(0)), Null(INT32), ConstInt32(author_id_ref)));

  scoped_ptr<CompoundExpression> tuple(new CompoundExpression());
  tuple->AddAs("book_id", ConstInt32(book_id));
  tuple->AddAs("author_id_ref", author_id_ref_or_null.release());
  tuple->AddAs("title", ConstString(title));
  tuple->AddAs("date_published", date_or_null.release());

  scoped_ptr<Operation> compute(Compute(tuple.release(), Generate(1)));
  scoped_ptr<Cursor> cursor(SucceedOrDie(compute->CreateCursor()));
  ResultView result = cursor->Next(1);
  if (result.has_data()) {
    TableSink sink(book);
    rowcount_t written_rows = SucceedOrDie(sink.Write(result.view()));
  }
  return result.view().column(0).typed_data<INT32>()[0];
}

static void fillData(Table *author, Table *book) {
  int terry_id = addAuthor(author, 1, "Terry Pratchett", false);
  int chuck_id = addAuthor(author, 2, "Chuck Palahniuk", false);
  int ernest_id = addAuthor(author, 3, "Ernest Hemingway", true);  int book_id = 1;
  addBook(book, book_id++, terry_id, "The Reaper Man", "1991/01/01");
  addBook(book, book_id++, terry_id, "Colour of Magic", "1983/01/01");
  addBook(book, book_id++, terry_id, "Light Fantastic", "1986/01/01");
  addBook(book, book_id++, terry_id, "Mort", NULL);

  addBook(book, book_id++, chuck_id, "Fight Club", "1996/01/01");
  addBook(book, book_id++, chuck_id, "Survivor", NULL);
  addBook(book, book_id++, chuck_id, "Choke", "2001/01/01");

  addBook(book, book_id++, ernest_id, "The old man and the sea", NULL);
  addBook(book, book_id++, ernest_id, "For whom the bell tolls", NULL);
  addBook(book, book_id++, ernest_id, "A farewell to arms", "1929/01/01");

  addBook(book, book_id++, -1, "Carpet People", NULL);
  addBook(book, book_id++, -1, "Producing open source software.", NULL);
  addBook(book, book_id++, -1, "Quantum computation and quantum information.", NULL);
}

static void LearnHashJoin() {
  scoped_ptr<Table> author(createAuthorTable());
  scoped_ptr<Table> book(createBookTable());
  fillData(author.get(), book.get());

  // select title, date_published, name as author_name, nobel from author inner join book on author_id = author_id_ref
  // HashJoinOperation (

  scoped_ptr<const SingleSourceProjector> book_selector(ProjectNamedAttribute("author_id_ref"));
  scoped_ptr<const SingleSourceProjector> author_selector(ProjectNamedAttribute("author_id"));
  scoped_ptr<CompoundMultiSourceProjector> result_projector(new CompoundMultiSourceProjector());

  scoped_ptr<CompoundSingleSourceProjector> result_book_projector(new CompoundSingleSourceProjector());
  result_book_projector->add(ProjectNamedAttribute("title"));
  result_book_projector->add(ProjectNamedAttribute("date_published"));

  scoped_ptr<CompoundSingleSourceProjector> result_author_projector(new CompoundSingleSourceProjector());
  result_author_projector->add(ProjectNamedAttributeAs("name", "author_name"));
  result_author_projector->add(ProjectNamedAttribute("nobel"));

  result_projector->add(0, result_book_projector.release());
  result_projector->add(1, result_author_projector.release());

  scoped_ptr<Operation> hash_join(new HashJoinOperation(INNER,
        book_selector.release(), author_selector.release(),
        result_projector.release(),
        UNIQUE,
        ScanView(book->view()),
        ScanView(author->view())
      ));
  scoped_ptr<Cursor> cursor(SucceedOrDie(hash_join->CreateCursor()));
  ResultView result = cursor->Next(-1);
  if (result.has_data()) {
    const View& view = result.view();
    const StringPiece* title = view.column(0).typed_data<STRING>();
    const int* date_published = view.column(1).typed_data<DATE>();
    cout << view.schema().GetHumanReadableSpecification() << endl;
    for (int i = 0; i < view.row_count(); i++) {
      cout << title[i] << "\t" << date_published[i] << endl;
    }
  }
}


// Generate only row count, but no any column.
static void LearnGenerate() {
  scoped_ptr<const Expression> rand(RandInt32());
  scoped_ptr<Operation> genRand(Compute(rand.release(), Generate(20)));
  scoped_ptr<Cursor> cursor(SucceedOrDie(genRand->CreateCursor()));
  scoped_ptr<ResultView> rv(new ResultView(cursor->Next(5)));
  assert(rv->has_data());
  cout << rv->view().schema().GetHumanReadableSpecification() << endl;
  while (!rv->is_done()) {
    const int* d = rv->view().column(0).typed_data<INT32>();
    for (int i = 0; i < rv->view().row_count(); i++)
      cout << d[i] << "," << endl;
    rv.reset(new ResultView(cursor->Next(5)));
  }
}

static void LearnSimpleFilter() {
  scoped_ptr<Table> table(new Table(TupleSchema::Singleton("num", INT32, NOT_NULLABLE),
      HeapBufferAllocator::Get()));
  TableRowWriter writer(table.get());
  for (int i = 0; i < 1000; i++)
    writer.AddRow().Set<INT32>(i).CheckSuccess();

  // create data source
  scoped_ptr<Operation> data_source(ScanView(table->view()));
  // create predicate
  scoped_ptr<const Expression> is_odd(IsOdd(AttributeAt(0)));
  // which column to filter
  scoped_ptr<const SingleSourceProjector> projector(ProjectNamedAttribute("num"));
  // create filter
  scoped_ptr<Operation> filter(Filter(is_odd.release(), projector.release(), data_source.release()));
  filter->SetBufferAllocator(HeapBufferAllocator::Get(), false);
  // evaluate and dump result.
  scoped_ptr<Cursor> cursor(SucceedOrDie(filter->CreateCursor()));
  scoped_ptr<ResultView> result(new ResultView(cursor->Next(-1)));
  if (result->has_data() && !result->is_eos()) {
    const View& view = result->view();
    cout << view.schema().GetHumanReadableSpecification() << endl;
    cout << view.row_count() << endl;
  }
}

static void LearnGetHumanReadableSpecification() {
  TupleSchema schema;
  schema.add_attribute(Attribute("name", STRING, NOT_NULLABLE));
  schema.add_attribute(Attribute("age", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("salary", DOUBLE, NOT_NULLABLE));
  schema.add_attribute(Attribute("full_time", BOOL, NOT_NULLABLE));
  schema.add_attribute(Attribute("dept", STRING, NOT_NULLABLE));
  cout << schema.GetHumanReadableSpecification() << endl;
}

static void LearnCopier() {
  using supersonic::ViewCopier;
  using supersonic::NO_SELECTOR;

  TupleSchema schema = TupleSchema::Singleton("id", INT32, NOT_NULLABLE);

  const int row_count = 10000;
  vector<int> a(row_count);
  for (int i = 0; i < row_count; i++)
    a[i] = i;
  View view(schema);
  view.set_row_count(row_count);
  view.mutable_column(0)->Reset(&a[0], NULL);
  ViewCopier copier(schema, schema, NO_SELECTOR, true);
  scoped_ptr<Block> result_space(new Block(schema, HeapBufferAllocator::Get()));
  result_space->Reallocate(row_count);

  scoped_ptr<Cursor> cursor(SucceedOrDie(ScanView(view)->CreateCursor()));
  unsigned offset = 0;
  scoped_ptr<ResultView> rv(new ResultView(cursor->Next(1024)));
  while (!rv->is_done()) {
    const View& view = rv->view();
    unsigned view_row_count = view.row_count();
    unsigned copied = copier.Copy(view_row_count, view, NULL, offset, result_space.get());
    cout << "copied " << view_row_count << endl;
    assert(copied == view_row_count);
    rv.reset(new ResultView(cursor->Next(1024)));
  }
  cout << "total: " << result_space->view().row_count() << endl;
}


static void LearnRandGen() {
  Table table(TupleSchema::Singleton("rnd", INT32, NOT_NULLABLE), HeapBufferAllocator::Get());
  TableRowWriter writer(&table);
  for (int i = 0; i < 10; i++)
    writer.AddRow().Set<INT32>(i).CheckSuccess();

  Operation* rand = Compute(RandInt32(), &table);
  scoped_ptr<Cursor> cursor(SucceedOrDie(rand->CreateCursor()));
  scoped_ptr<ResultView> result(new ResultView(cursor->Next(10)));
  if (result->has_data() && !result->is_eos()) {
    cout << result->view().row_count() << endl;
  } else {
    cout << "NO DATA" << endl;
  }
}

static void LearnSmallSort() {
  TupleSchema schema;
  schema.add_attribute(Attribute("id", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("grade", INT32, NOT_NULLABLE));

  scoped_ptr<View> input_view(new View(schema));
  const unsigned row_count = 8;
  int ids[row_count] = {1,2,3,4,5,6,7,8};
  int grades[row_count] = {4,1,2,3,6,8,9,3};

  input_view->set_row_count(row_count);
  input_view->mutable_column(0)->Reset(ids, NULL);
  input_view->mutable_column(1)->Reset(grades, NULL);

  scoped_ptr<const SingleSourceProjector> projector(ProjectNamedAttribute("grade"));
  scoped_ptr<SortOrder> order(new SortOrder());
  order->add(projector.release(), ASCENDING);
  const size_t mem_limit = 128;
  scoped_ptr<Operation> sort(Sort(order.release(), NULL, mem_limit, ScanView(*input_view)));
  scoped_ptr<Cursor> cursor(SucceedOrDie(sort->CreateCursor()));
  ResultView result = cursor->Next(-1);
  if (result.has_data() && !result.is_eos()) {
    View result_view = result.view();
    for (int row = 0; row < result_view.row_count(); row++) {
      cout << result_view.column(0).typed_data<INT32>()[row] << ",";
      cout << result_view.column(1).typed_data<INT32>()[row] << "\n";
    }
  }
}

static void LearnGroupAggregate() {
  // create view
  TupleSchema schema;
  schema.add_attribute(Attribute("name", STRING, NOT_NULLABLE));
  schema.add_attribute(Attribute("age", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("salary", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("dept", STRING, NOT_NULLABLE));
  schema.add_attribute(Attribute("full_time", BOOL, NOT_NULLABLE));

  View input_view(schema);

  // load data
  Arena arena(32, 128);
  const unsigned row_count = 5;
  string names_str[row_count] = {"John", "Darrel", "Greg", "Amanda", "Stacy"};
  int32 ages[row_count] = {20, 25, 32, 31, 33};
  int32 salaries[row_count] = {1800, 3300, 4800, 3500, 1900};
  string depts_str[row_count] = {"Accounting", "Sales", "Sales", "IT", "IT"};
  bool full_times[row_count] = {false, true, false, true, false};

  StringPiece names[row_count];
  StringPiece depts[row_count];
  for (int i = 0; i < row_count; i++) {
    names[i] = StringPiece(arena.AddStringPieceContent(StringPiece(names_str[i])), names_str[i].length());
    depts[i] = StringPiece(arena.AddStringPieceContent(StringPiece(depts_str[i])), depts_str[i].length());
  }
  input_view.set_row_count(row_count);
  input_view.mutable_column(0)->Reset(names, NULL);
  input_view.mutable_column(1)->Reset(ages, NULL);
  input_view.mutable_column(2)->Reset(salaries, NULL);
  input_view.mutable_column(3)->Reset(depts, NULL);
  input_view.mutable_column(4)->Reset(full_times, NULL);

  // create grouped aggregate operation
  scoped_ptr<AggregationSpecification> specification(new AggregationSpecification());
  specification->AddAggregation(MAX, "salary", "max_salary");
  specification->AddAggregation(MIN, "age", "min_age");
  scoped_ptr<CompoundSingleSourceProjector> projector(new CompoundSingleSourceProjector());
  projector->add(ProjectNamedAttribute("dept"));
  projector->add(ProjectAttributeAt(4));
  scoped_ptr<Operation> aggregation(GroupAggregate(projector.release(), specification.release(), NULL,
      ScanView(input_view)));

  scoped_ptr<Cursor> cursor(SucceedOrDie(aggregation->CreateCursor()));
  ResultView result = cursor->Next(-1);
  if (result.has_data() && !result.is_eos()) {
    View result_view = result.view();
    for (int col = 0; col < result_view.column_count(); col++) {
      std::cout << result_view.column(col).attribute().name() << "\t";
    }
    std::cout << std::endl;
    for (int row = 0; row < result_view.row_count(); row++) {
      std::cout << result_view.column(0).typed_data<STRING>()[row] << "\t";
      std::cout << result_view.column(1).typed_data<BOOL>()[row] << "\t";
      std::cout << result_view.column(2).typed_data<INT32>()[row] << "\t";
      std::cout << result_view.column(3).typed_data<INT32>()[row] << "\t";
      std::cout << std::endl;
    }
  }
}

static void LearnStringColumn() {
  TupleSchema schema;
  schema.add_attribute(Attribute("a", STRING, NOT_NULLABLE));
  string names_str[5] = {
      "jack", "boby", "tomas", "jobs", "bill"
  };
  StringPiece names[5];

  Arena arena(32, 128);
  for (int i = 0; i < 5; i++) {
    names[i] = StringPiece(arena.AddStringPieceContent(StringPiece(names_str[i])), names_str[i].length());
    std::cout << i << ":" << names[i].as_string() << std::endl;
  }
  View input_view(schema);
  input_view.set_row_count(5);
  input_view.mutable_column(0)->Reset(names, NULL);
  for (int i = 0; i < input_view.row_count(); i++)
    std::cout << input_view.column(0).typed_data<STRING>()[i].as_string() << std::endl;
}


static void LearnScanView() {
  TupleSchema schema;
  schema.add_attribute(Attribute("a", INT32, NOT_NULLABLE));
  View input_view(schema);

  const int a[5] = {1,2,3,4,5};
  input_view.set_row_count(5);
  input_view.mutable_column(0)->Reset(a, NULL);
  scoped_ptr<Operation> scan(ScanView(input_view));
  scoped_ptr<Cursor> cursor(SucceedOrDie(scan->CreateCursor()));
  ResultView result = cursor->Next(-1);
  if(result.has_data()) {
    View view = result.view();
    std::cout << view.column_count() << std::endl;
    std::cout << view.row_count() << std::endl;
    for (int i = 0; i < view.row_count(); i++) {
        std::cout << view.column(0).typed_data<INT32>()[i] << std::endl;
    }
  }
}


static void LearnGroupExpr() {
  int keys[10] = {1,2,3,1,2,3,1,1,2,3};
  double prices[10] = {1.1,1.2,1.3,2,2,2,3,3,3,1};

  TupleSchema schema;
  schema.add_attribute(Attribute("key", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("prices", DOUBLE, NOT_NULLABLE));

  View input_view(schema);
  input_view.set_row_count(10);
  input_view.mutable_column(0)->Reset(keys, NULL);
  input_view.mutable_column(1)->Reset(prices, NULL);

  scoped_ptr<AggregationSpecification> specification(
      new AggregationSpecification());
  specification->AddAggregation(SUM, "prices", "price_sums");

  scoped_ptr<const SingleSourceProjector>
    key_projector(ProjectNamedAttribute("key"));

  scoped_ptr<Operation> aggregation(GroupAggregate(key_projector.release(),
      specification.release(),
      NULL,
      ScanView(input_view)));

  scoped_ptr<Cursor> bound_aggregation(SucceedOrDie(aggregation->CreateCursor()));
  ResultView result(bound_aggregation->Next(-1));
  if (result.has_data() && !result.is_eos()) {
    View result_view(result.view());
    std::cout << "column_count: " << result_view.column_count() << std::endl;
    std::cout << "row_count:" << result_view.row_count() << std::endl;

    const int *key = result_view.column(0).typed_data<INT32>();
    const double *sums = result_view.column(1).typed_data<DOUBLE>();

    for (int i = 0; i < result_view.row_count(); i++) {
      std::cout << key[i] << ", " << sums[i] << std::endl;
    }
  }
}


static void LearnRandExpr() {
  const Expression* rand = RandInt32();
  Table table(TupleSchema::Singleton("a", INT32, NULLABLE),
      HeapBufferAllocator::Get());

  TableRowWriter writer(&table);
  writer.AddRow().Int32(12).CheckSuccess();

  Operation* compute = Compute(rand, &table);
  compute->SetBufferAllocator(HeapBufferAllocator::Get(), false);

  FailureOrOwned<Cursor> cursor = compute->CreateCursor();
  assert(cursor.is_success());
  ResultView output = cursor.get()->Next(1);
  assert(output.has_data());
  View view = SucceedOrDie(output);
  const int *p = view.column(0).typed_data<INT32>();
  printf("%d\n", p[0]);
}

static void LearnAddExpr() {
  const Expression* col_a = NamedAttribute("a");
  const Expression* col_b = NamedAttribute("b");
  const Expression* plus = Plus(col_a, col_b);
  Table table(TupleSchema::Merge(
      TupleSchema::Singleton("a", INT32, NULLABLE),
      TupleSchema::Singleton("b", INT32, NULLABLE)), HeapBufferAllocator::Get());
  TableRowWriter writer(&table);
  writer.AddRow().Int32(1).Int32(2)
        .AddRow().Int32(2).Int32(3)
        .CheckSuccess();
  Operation* computation = Compute(plus, &table);
  computation->SetBufferAllocator(HeapBufferAllocator::Get(), false);
  FailureOrOwned<Cursor> cursor = computation->CreateCursor();
  assert(cursor.is_success());
  ResultView output = cursor.get()->Next(2);
  assert(output.has_data());
  View view = SucceedOrDie(output);
  const int* p = view.column(0).typed_data<INT32>();
  printf("%d %d\n", p[0], p[1]);
}


static void LearnSimpleExpr() {
  // construct a plus expression
  scoped_ptr<const Expression> addition(Plus(AttributeAt(0), AttributeAt(1)));
  TupleSchema schema;
  schema.add_attribute(Attribute("a", INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("b", INT32, NOT_NULLABLE));
  FailureOrOwned<BoundExpressionTree> bound_addition =
      addition->Bind(schema, HeapBufferAllocator::Get(), 2048);
  if (!bound_addition.is_success()) {
    std::cout << bound_addition.exception().message();
    return;
  }

  // construct input
  int a[10] = {1,2,3,4,5,6,7,8,9,0};
  int b[10] = {1,2,3,4,5,6,7,8,9,10};
  TupleSchema input_schema;
  input_schema.add_attribute(Attribute("a", INT32, NOT_NULLABLE));
  input_schema.add_attribute(Attribute("b", INT32, NOT_NULLABLE));
  View input_view(input_schema);
  input_view.set_row_count(10);

  input_view.mutable_column(0)->Reset(a, NULL);
  input_view.mutable_column(1)->Reset(b, NULL);

  EvaluationResult result = bound_addition->Evaluate(input_view);
  if (!result.is_success()) {
    std::cout << result.exception().message();
    return;
  }

  std::cout << "row_count:" << result.get().row_count() << std::endl;
  std::cout << "col_count:" << result.get().column_count() << std::endl;

  const int* data = result.get().column(0).typed_data<INT32>();
  for (size_t i = 0; i < result.get().row_count(); i++) {
    std::cout << data[i] << std::endl;
  }
}

int main(int argc, char** argv) {
  supersonic::SupersonicInit(&argc, &argv);

  cout << "LearnGetHumanReadableSpecification()" << endl;
  LearnGetHumanReadableSpecification();
  cout << "LearnRandExpr" << endl;
  LearnRandExpr();
  cout << "LearnAddExpr" << endl;
  LearnAddExpr();
  cout << "LearnSimpleExpr" << endl;
  LearnSimpleExpr();
  cout << "LearnGroupExpr" << endl;
  LearnGroupExpr();
  cout << "LearnScanView" << endl;
  LearnScanView();
  cout << "LearnStringColumn" << endl;
  LearnStringColumn();
  cout << "LearnGroupAggregate" << endl;
  LearnGroupAggregate();
  cout << "LearnSort" << endl;
  LearnSmallSort();

  cout << "LearnRandGen" << endl;
  LearnRandGen();

  cout << "LearnCopier" << endl;
  LearnCopier();

  cout << "LearnSimpleFilter" << endl;
  LearnSimpleFilter();

  cout << "LearnGenerate Opeartion" << endl;
  LearnGenerate();

  cout << "LearnHashJoin" << endl;
  LearnHashJoin();
  return 0;
}
