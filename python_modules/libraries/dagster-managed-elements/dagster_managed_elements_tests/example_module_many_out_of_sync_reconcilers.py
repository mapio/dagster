from dagster_managed_elements import ManagedElementDiff
from dagster_managed_elements_tests.example_reconciler import MyManagedElementReconciler

my_reconciler = MyManagedElementReconciler(
    ManagedElementDiff()
    .add("foo", "bar")
    .add("same", "as")
    .with_nested("nested", ManagedElementDiff().add("qwerty", "uiop").add("new", "field"))
)

my_other_reconciler = MyManagedElementReconciler(ManagedElementDiff().delete("foo", "bar"))
